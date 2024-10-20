<?php
namespace GWSN\FlysystemSharepoint;

use Exception;
use RuntimeException;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use Microsoft\Graph\Exception\GraphException;
use Throwable;

class FlysystemSharepointAdapter implements FilesystemAdapter
{
    private string $prefix;

    private SharepointConnector $connector;

    public function __construct(
        SharepointConnector $connector,
        string $prefix = '/'
    )
    {
        $this->setConnector($connector);
        $this->setPrefix($prefix);
    }

    /**
     * @return SharepointConnector
     */
    public function getConnector(): SharepointConnector
    {
        return $this->connector;
    }

    /**
     * @param SharepointConnector $connector
     * @return FlysystemSharepointAdapter
     */
    public function setConnector(SharepointConnector $connector): FlysystemSharepointAdapter
    {
        $this->connector = $connector;
        return $this;
    }

    /**
     * @return string
     */
    public function getPrefix(): string
    {
        return $this->prefix;
    }

    /**
     * @param string $prefix
     * @return FlysystemSharepointAdapter
     */
    public function setPrefix(string $prefix): FlysystemSharepointAdapter
    {
        $this->prefix = sprintf('/%s', trim($prefix, '/'));
        return $this;
    }

    /**
     * @param string $path
     * @return bool
     * @throws \Exception
     */
    public function fileExists(string $path): bool
    {
        return $this->connector->getFile()->checkFileExists($this->applyPrefix($path));
    }

    /**
     * @param string $path
     * @return bool
     * @throws \Exception
     */
    public function directoryExists(string $path): bool
    {
        return $this->connector->getFolder()->checkFolderExists($this->applyPrefix($path));
    }

    /**
     * @param string $path
     * @param string $contents
     * @param Config $config
     * @return void
     * @throws \Exception
     */
    public function write(string $path, string $contents, Config $config): void
    {
        //Files larger than 4MiB require an UploadSession
        if (strlen($contents) > (4 * 1024 * 1024)) {
            $stream = fopen('php://temp', 'r+');
            fwrite($stream, $contents);
            rewind($stream);
            $this->writeStream($path, $stream, $config);
        } else {
            $mimeType = $config->get('mimeType', 'text/plain');
            $this->connector->getFile()->writeFile($this->applyPrefix($path), $contents, $mimeType);
        }
    }

    /**
     * Snippet heavily inspired by: https://github.com/shitware-ltd/flysystem-msgraph
     *
     * @param string $path
     * @param $contents
     * @param Config $config
     * @return void
     * @throws Exception
     * @throws GuzzleException
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $uploadUrl = $this->createUploadSession($path);

        $meta = fstat($contents) ?: throw new UnableToReadFile('Failed to get information about the file using the open file pointer');
        $chunkSize = $config->withDefaults(['chunk_size' => 320 * 1024 * 10])->get('chunk_size');
        $offset = 0;

        //Chunks have to be uploaded without authorization headers, so we need a fresh guzzle client
        $guzzle = new Client();
        while ($chunk = fread($contents, $chunkSize)) {
            $this->writeChunk($guzzle, $uploadUrl, $meta['size'], $chunk, $offset);
            $offset += $chunkSize;
        }
    }

    /**
     * Snippet heavily inspired by: https://github.com/shitware-ltd/flysystem-msgraph
     *
     * @throws GuzzleException
     * @throws GraphException
     * @throws RuntimeException
     */
    private function writeChunk(Client $guzzle, string $upload_url, int $file_size, string $chunk, int $first_byte, int $retries = 0): void
    {
        $last_byte_pos = $first_byte + strlen($chunk) - 1;
        $headers = [
            'Content-Range' => "bytes $first_byte-$last_byte_pos/$file_size",
            'Content-Length' => strlen($chunk),
        ];
        $response = $guzzle->request('PUT', $upload_url, [
            'headers' => $headers,
            'body' => $chunk,
            'timeout' => 120,
        ]);
        if ($response->getStatusCode() === 404) {
            throw new RuntimeException('Upload URL has expired, please create new upload session');
        }
        if ($response->getStatusCode() === 429) {
            sleep($response->getHeader('Retry-After')[0] ?? 1);
            $this->writeChunk($guzzle, $upload_url, $file_size, $chunk, $first_byte, $retries + 1);
        }
        if ($response->getStatusCode() >= 500) {
            if ($retries > 3) {
                throw new RuntimeException('Upload failed after 10 attempts.');
            }
            sleep(pow(2, $retries));
            $this->writeChunk($guzzle, $upload_url, $file_size, $chunk, $first_byte, $retries + 1);
        }
        if (($file_size - 1) == $last_byte_pos) {
            if ($response->getStatusCode() === 409) {
                throw new RuntimeException('File name conflict. A file with the same name already exists at target destination.');
            }
            if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 500) {
                return;
            }
            $errorMsg = 'Microsoft Graph Request: Failed request, expected the returnCode 200 but actual %s';
            throw new RuntimeException(sprintf($errorMsg, $response->getStatusCode()), $response->getStatusCode());
        }
        if ($response->getStatusCode() !== 202) {
            throw new RuntimeException('Unknown error occurred while trying to upload file chunk. HTTP status code is ' . $response->getStatusCode());
        }
    }

    /**
     * Snippet heavily inspired by: https://github.com/shitware-ltd/flysystem-msgraph
     *
     * @throws Exception
     */
    public function createUploadSession(string $path): ?string
    {
        $requestUrl = $this->getFileUrl($path) . ':/createUploadSession';
        $response = $this->connector->getFile()->getApiConnector()->request('POST', $requestUrl);
        return $response['uploadUrl'] ?? null;
    }

    /**
     * @throws Exception
     */
    public function getFileUrl(string $path): string
    {
        $parent = explode('/', $path);
        $fileName = array_pop($parent);

        // Create parent folders if not exists
        $parentFolder = sprintf('/%s', ltrim(implode('/', $parent), '/'));
        if ($parentFolder !== '/') {
            $this->connector->getFolder()->createFolderRecursive($parentFolder);
        }

        $parentFolderMeta = $this->connector->getFolder()->requestFolderMetadata($parentFolder);
        $parentFolderId = $parentFolderMeta['id'];

        return $this->getFileBaseUrl(null, $parentFolderId, sprintf(':/%s', $fileName));
    }

    /**
     * @param string $path
     * @return string
     * @throws \Exception
     */
    public function read(string $path): string
    {
        return $this->connector->getFile()->readFile($this->applyPrefix($path));
    }

    /**
     * @param string $path
     * @return resource
     * @throws \Exception
     */
    public function readStream(string $path)
    {
        $path = $this->applyPrefix($path);
        /** @var resource $readStream */
        $readStream = fopen($this->connector->getFile()->requestFileStreamUrl($path), 'rb');

        if (! $readStream) {
            fclose($readStream);
            throw UnableToReadFile::fromLocation($path);
        }

        return $readStream;
    }

    /**
     * @param string $path
     * @return void
     * @throws \Exception
     */
    public function delete(string $path): void
    {
        $this->connector->getFile()->deleteFile($this->applyPrefix($path));
    }

    /**
     * @param string $path
     * @return void
     * @throws \Exception
     */
    public function deleteDirectory(string $path): void
    {
        $this->connector->getFolder()->deleteFolder($this->applyPrefix($path));
    }

    /**
     * @param string $path
     * @param Config $config
     * @return void
     * @throws \Exception
     */
    public function createDirectory(string $path, Config $config): void
    {
        $this->connector->getFolder()->createFolderRecursive($this->applyPrefix($path));
    }

    /**
     * @param string $path
     * @param string $visibility
     * @return void
     * @throws \Exception
     */
    public function setVisibility(string $path, string $visibility): void
    {
        throw new \Exception('Function not implemented');
    }

    /**
     * @param string $path
     * @return FileAttributes
     * @throws \Exception
     */
    public function visibility(string $path): FileAttributes
    {
        // TODO: Implement visibility() method.
        throw new \Exception('Function not implemented');
    }

    /**
     * @param string $path
     * @return FileAttributes
     */
    public function mimeType(string $path): FileAttributes
    {
        $path = $this->applyPrefix($path);

        try {
            $mimetype = $this->connector->getFile()->checkFileMimeType($path);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::mimeType($path, $exception->getMessage(), $exception);
        }

        if ($mimetype === null) {
            throw UnableToRetrieveMetadata::mimeType($path, 'Unknown.');
        }

        return new FileAttributes(
            path: $path,
            mimeType: $mimetype
        );
    }

    /**
     * @param string $path
     * @return FileAttributes
     * @throws \Exception
     */
    public function lastModified(string $path): FileAttributes
    {
        $path = $this->applyPrefix($path);

        try {
            $lastModified = $this->connector->getFile()->checkFileLastModified($path);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::lastModified($path, $exception->getMessage(), $exception);
        }

        if ($lastModified === null) {
            throw UnableToRetrieveMetadata::lastModified($path, 'Unknown.');
        }

        return new FileAttributes(
            path: $path,
            lastModified: $lastModified
        );
    }

    /**
     * @param string $path
     * @return FileAttributes
     * @throws \Exception
     */
    public function fileSize(string $path): FileAttributes
    {
        $path = $this->applyPrefix($path);

        try {
            $fileSize = $this->connector->getFile()->checkFileSize($path);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::fileSize($path, $exception->getMessage(), $exception);
        }

        if ($fileSize === null) {
            throw UnableToRetrieveMetadata::fileSize($path, 'Unknown.');
        }

        return new FileAttributes(
            path: $path,
            fileSize: $fileSize
        );
    }

    /**
     * @param string $path
     * @param bool $deep
     * @return iterable|StorageAttributes[]
     * @throws \Exception
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $content = [];
        $result = $this->connector->getFolder()->requestFolderItems($this->applyPrefix($path));

        if(count($result) > 0) {
            foreach($result as $value) {
                if(isset($value['folder'])) {
                    $content[] = new DirectoryAttributes($value['name'], 'notSupported', (new \DateTime($value['lastModifiedDateTime']))->getTimestamp(), $value);
                }
                if(isset($value['file'])) {
                    $content[] = new FileAttributes($value['name'], $value['size'], 'notSupported', (new \DateTime($value['lastModifiedDateTime']))->getTimestamp(), $value['file']['mimeType'], $value);
                }
            }
        }

        return $content;
    }

    /**
     * @param string $source
     * @param string $destination
     * @param Config $config
     * @return void
     * @throws \Exception
     */
    public function move(string $source, string $destination, Config $config): void
    {
        $parent = explode('/', $destination);
        $fileName = array_pop($parent);

        // Create parent folders if not exists
        $parentFolder = sprintf('/%s', ltrim(implode('/', $parent), '/'));

        $this->connector->getFile()->moveFile($this->applyPrefix($source), $this->applyPrefix($parentFolder), $fileName);
    }

    /**
     * @param string $source
     * @param string $destination
     * @param Config $config
     * @return void
     * @throws \Exception
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        $parent = explode('/', $destination);
        $fileName = array_pop($parent);

        // Create parent folders if not exists
        $parentFolder = sprintf('/%s', ltrim(implode('/', $parent), '/'));

        $this->connector->getFile()->copyFile($this->applyPrefix($source), $this->applyPrefix($parentFolder), $fileName);
    }
    
    private function applyPrefix(string $path): string {
        if($path === '' || $path === '/'){
            return $this->getPrefix();
        }
        return sprintf('%s/%s', $this->getPrefix(), ltrim($path));
    }
}
