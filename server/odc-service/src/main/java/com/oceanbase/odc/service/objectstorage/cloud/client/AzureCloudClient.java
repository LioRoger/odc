/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.odc.service.objectstorage.cloud.client;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.util.ProgressListener;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.oceanbase.odc.service.objectstorage.cloud.model.CloudObjectStorageConstants;
import com.oceanbase.odc.service.objectstorage.cloud.model.CompleteMultipartUploadRequest;
import com.oceanbase.odc.service.objectstorage.cloud.model.CompleteMultipartUploadResult;
import com.oceanbase.odc.service.objectstorage.cloud.model.CopyObjectResult;
import com.oceanbase.odc.service.objectstorage.cloud.model.DeleteObjectRequest;
import com.oceanbase.odc.service.objectstorage.cloud.model.DeleteObjectsRequest;
import com.oceanbase.odc.service.objectstorage.cloud.model.DeleteObjectsResult;
import com.oceanbase.odc.service.objectstorage.cloud.model.GetObjectRequest;
import com.oceanbase.odc.service.objectstorage.cloud.model.InitiateMultipartUploadRequest;
import com.oceanbase.odc.service.objectstorage.cloud.model.InitiateMultipartUploadResult;
import com.oceanbase.odc.service.objectstorage.cloud.model.ObjectMetadata;
import com.oceanbase.odc.service.objectstorage.cloud.model.ObjectSummary;
import com.oceanbase.odc.service.objectstorage.cloud.model.ObjectTagging;
import com.oceanbase.odc.service.objectstorage.cloud.model.PutObjectResult;
import com.oceanbase.odc.service.objectstorage.cloud.model.StorageObject;
import com.oceanbase.odc.service.objectstorage.cloud.model.UploadObjectTemporaryCredential;
import com.oceanbase.odc.service.objectstorage.cloud.model.UploadPartRequest;
import com.oceanbase.odc.service.objectstorage.cloud.model.UploadPartResult;

/**
 * @author longpeng.zlp
 * @date 2025/4/24 15:05
 */
public class AzureCloudClient implements CloudClient {
    private final BlobServiceClient blobServiceClient;

    public AzureCloudClient(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    @Override
    public boolean supported() {
        return true;
    }

    @Override
    public String getBucketLocation(String bucketName) throws CloudException {
        return null;
    }

    @Override
    public boolean doesBucketExist(String bucketName) throws CloudException {
        return callAzureMethod("does bucket exist " + bucketName, () -> {
            BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(bucketName);
            return blobContainerClient.exists();
        } );
    }

    public boolean supportPartUpload() {
        return false;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
        throws CloudException {
        throw new IllegalStateException("not support part upload");
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws CloudException {
        throw new IllegalStateException("not support part upload");
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
        throws CloudException {
        throw new IllegalStateException("not support part upload");
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file, ObjectMetadata metadata)
        throws CloudException {
        return callAzureMethod("put object " + bucketName + "." + file.getName(), () -> {
            BlobClient client = getBlobClient(bucketName, key);
            BlobUploadFromFileOptions options = buildUploadFromFileOptions(file, metadata);
            Response<BlockBlobItem> response = client.uploadFromFileWithResponse(options, null, null);
            return fromUploadResponse(response);
        });
    }

    protected PutObjectResult fromUploadResponse(Response<BlockBlobItem> response) {
        BlockBlobItem blockBlobItem = response.getValue();
        PutObjectResult ret = new PutObjectResult();
        ret.setETag(blockBlobItem.getETag());
        ret.setVersionId(blockBlobItem.getVersionId());
        ret.setRequestId(response.getRequest().getUrl().toString());
        return ret;
    }

    protected BlobUploadFromFileOptions buildUploadFromFileOptions(File file, ObjectMetadata metadata) {
        BlobUploadFromFileOptions blobUploadFromFileOptions = new BlobUploadFromFileOptions(file.getAbsolutePath());
        blobUploadFromFileOptions.setParallelTransferOptions(buildParallelTransferOptions());
        fillOptions(blobUploadFromFileOptions::setMetadata, blobUploadFromFileOptions::setTags, blobUploadFromFileOptions::setHeaders, metadata);
        return blobUploadFromFileOptions;
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream in, ObjectMetadata metadata)
        throws CloudException {
        return callAzureMethod("put object " + bucketName + "." + key, () -> {
            BlobClient client = getBlobClient(bucketName, key);
            BlobParallelUploadOptions options = buildUploadInputStreamOptions(in, metadata);
            Response<BlockBlobItem> response = client.uploadWithResponse(options, null, null);
            return fromUploadResponse(response);
        });
    }

    protected BlobParallelUploadOptions buildUploadInputStreamOptions(InputStream stream, ObjectMetadata metadata) {
        BlobParallelUploadOptions blobParallelUploadOptions = new BlobParallelUploadOptions(stream);
        blobParallelUploadOptions.setParallelTransferOptions(buildParallelTransferOptions());
        fillOptions(blobParallelUploadOptions::setMetadata, blobParallelUploadOptions::setTags, blobParallelUploadOptions::setHeaders, metadata);
        return blobParallelUploadOptions;
    }

    protected void fillOptions(Consumer<Map<String, String>> metadataConsumer,
        Consumer<Map<String, String>> tagConsumer,
        Consumer<BlobHttpHeaders> headersConsumer,
        ObjectMetadata metadata) {
        if (null == metadata) {
            return;
        }
        if (null != metadata.getUserMetadata()) {
            metadataConsumer.accept(metadata.getUserMetadata());
        }
        if (metadata.hasTag()) {
            tagConsumer.accept(castTag(metadata));
        }
        BlobHttpHeaders httpHeaders = new BlobHttpHeaders();
        boolean headerValid = false;
        if (null != metadata.getContentType()) {
            httpHeaders.setContentType(metadata.getContentType());
            headerValid = true;
        }
        if (null != metadata.getContentMD5()) {
            httpHeaders.setContentMd5(metadata.getContentMD5().getBytes());
            headerValid = true;
        }
        if (headerValid) {
            headersConsumer.accept(httpHeaders);
        }
    }

    @Override
    public CopyObjectResult copyObject(String bucketName, String from, String to) throws CloudException {
        return callAzureMethod("copy object " + bucketName + "." + from + " to " + to, () -> {
            BlobClient srcClient = getBlobClient(bucketName, from);
            BlobClient destClient = getBlobClient(bucketName, to);
            destClient.copyFromUrl(srcClient.getBlobUrl());
            CopyObjectResult result = new CopyObjectResult();
            destClient = getBlobClient(bucketName, to);
            result.setETag(destClient.getProperties().getETag());
            result.setVersionId(destClient.getVersionId());
            result.setLastModifyTime(new Date(destClient.getProperties().getLastModified().toEpochSecond()));
            return result;
        });
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) throws CloudException {
        return callAzureMethod("delete objects " + request.getBucketName() + "." + StringUtils.join(request.getKeys(), ","), () -> {
            List<String> ret = new ArrayList<>();
            BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(request.getBucketName());
            for (String key : request.getKeys()) {
                BlobClient blobClient = blobContainerClient.getBlobClient(key);
                if (blobClient.deleteIfExists()) {
                    ret.add(key);
                }
            }
            DeleteObjectsResult result =  new DeleteObjectsResult();
            result.setDeletedObjects(ret);
            return result;
        });
    }

    @Override
    public String deleteObject(DeleteObjectRequest request) throws CloudException {
        return callAzureMethod("delete object " + request.getBucketName() + "." + request.getKey(), () -> {
            BlobClient blobClient = getBlobClient(request.getBucketName(), request.getKey());
            blobClient.deleteIfExists();
            return request.getKey();
        });
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key) throws CloudException {
        return callAzureMethod("does object exist " + bucketName + "." + key, () -> {
            BlobClient blobClient = getBlobClient(bucketName, key);
            return blobClient.exists();
        });
    }

    @Override
    public StorageObject getObject(String bucketName, String key) throws CloudException {
        return callAzureMethod("get object " + bucketName + "." + key, () -> {
            BlobClient blobClient =  getBlobClient(bucketName, key);
            BlobInputStream inputStream = blobClient.openInputStream();
            StorageObject ret =  new StorageObject() {
                @Override
                public InputStream getObjectContent() {
                    return inputStream;
                }

                @Override
                public InputStream getAbortableContent() {
                    return inputStream;
                }
            };
            ret.setBucketName(bucketName);
            ret.setKey(key);
            ret.setMetadata(getObjectMetadata(blobClient));
            return ret;
        });
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest request, File file) throws CloudException {
        return callAzureMethod("get object " + request.getKey() + " to " + file.getName(), () -> {
            BlobClient blobClient =  getBlobClient(request.getBucketName(), request.getKey());
            blobClient.downloadToFile(file.getAbsolutePath());
            return getObjectMetadata(blobClient);
        });
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key) throws CloudException {
        return callAzureMethod("get object metadata " + bucketName + "." + key, () -> {
            BlobClient blobClient = getBlobClient(bucketName, key);
            return getObjectMetadata(blobClient);
        });
    }

    protected ObjectMetadata getObjectMetadata(BlobClient client) {
        ObjectMetadata ret = new ObjectMetadata();
        ret.setContentType(client.getProperties().getContentType());
        ret.setETag(client.getProperties().getETag());
        ret.setUserMetadata(client.getProperties().getMetadata());
        ret.setContentLength(client.getProperties().getBlobSize());
        if (null != client.getProperties().getExpiresOn()) {
            ret.setExpirationTime(new Date(client.getProperties().getExpiresOn().toEpochSecond()));
        }
        if (null != client.getTags()) {
            ObjectTagging objectTagging = new ObjectTagging();
            client.getTags().forEach((k, v) -> {
                objectTagging.withTag(k, v);
            });
            ret.setTagging(objectTagging);
        }
        return ret;
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws CloudException {
        return generatePresignedUrlWithCustomFileName(bucketName, key, expiration, null);
    }

    @Override
    public URL generatePresignedUrlWithCustomFileName(String bucketName, String key, Date expiration,
        String customFileName) throws CloudException {
        return callAzureMethod("generate pregisgned url for " + bucketName + "." + key, () -> {
            BlobClient blobClient = getBlobClient(bucketName, key);
            BlobSasPermission permission = new BlobSasPermission();
            permission.setReadPermission(true);
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues()
                .setPermissions(permission);
            if (null != expiration) {
                sasValues.setExpiryTime(OffsetDateTime.ofInstant(expiration.toInstant(), ZoneId.systemDefault()));
            }
            if (!StringUtils.isEmpty(customFileName)) {
                sasValues.setContentDisposition(String.format("attachment;filename=%s", customFileName));
            }
            String sasToken = blobClient.generateSas(sasValues);
            String url = blobClient.getBlobUrl() + "?" + sasToken;
            return new URL(url);
        });
    }

    @Override
    public URL generatePresignedPutUrl(String bucketName, String key, Date expiration) throws CloudException {
        return callAzureMethod("generate pregisgned url for " + bucketName + "." + key, () -> {
            BlobClient blobClient = getBlobClient(bucketName, key);
            BlobSasPermission permission = new BlobSasPermission();
            permission.setWritePermission(true);
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues()
                .setPermissions(permission);
            if (null != expiration) {
                sasValues.setExpiryTime(OffsetDateTime.ofInstant(expiration.toInstant(), ZoneId.systemDefault()));
            }
            String sasToken = blobClient.generateSas(sasValues);
            String url = blobClient.getBlobUrl() + "?" + sasToken;
            return new URL(url);
        });
    }

    @Override
    public List<ObjectSummary> list(String bucketName, String prefix) throws CloudException {
        return callAzureMethod("list " + bucketName + "." + prefix, () -> {
            BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(bucketName);
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(prefix);
            PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(listBlobsOptions, null);
            List<ObjectSummary> ret = new ArrayList<>();
            for (BlobItem item : blobItems) {
                ObjectSummary tmp = new ObjectSummary();
                tmp.setBucketName(bucketName);
                tmp.setKey(item.getName());
                tmp.setETag(item.getProperties().getETag());
                tmp.setSize(item.getProperties().getContentLength());
                tmp.setType(item.getProperties().getContentType());
                tmp.setLastModified(new Date(item.getProperties().getLastModified().toEpochSecond()));
                tmp.setStorageClass(item.getProperties().getAccessTier().getValue());
                ret.add(tmp);
            }
            return ret;
        });
    }

    @Override
    public UploadObjectTemporaryCredential generateTempCredential(String bucketName, String objectName,
        Long durationSeconds) throws CloudException {
        return callAzureMethod("generate temp credential " + bucketName + "." + objectName, () -> {
            BlobClient blobClient = getBlobClient(bucketName, objectName);
            BlobSasPermission permission = BlobSasPermission.parse("acdelimrtwxy");
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues()
                .setPermissions(permission);
            if (null != durationSeconds) {
                sasValues.setExpiryTime(OffsetDateTime.ofInstant(Instant.now().plus(durationSeconds, ChronoUnit.SECONDS), ZoneId.systemDefault()));
            }
            String sasToken = blobClient.generateSas(sasValues);
            UploadObjectTemporaryCredential ret = new UploadObjectTemporaryCredential();
            ret.setSecurityToken(sasToken);
            return ret;
        });
    }

    protected BlobClient getBlobClient(String bucketName, String key) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(bucketName);
        return blobContainerClient.getBlobClient(key);
    }

    protected ParallelTransferOptions buildParallelTransferOptions() {
        ParallelTransferOptions ret =  new ParallelTransferOptions();
        ret.setBlockSizeLong(CloudObjectStorageConstants.MIN_PART_SIZE);
        ret.setMaxSingleUploadSizeLong(CloudObjectStorageConstants.CRITICAL_FILE_SIZE_IN_MB);
        ret.setMaxConcurrency(2);
        ret.setProgressListener(new ProgressListener() {
            @Override
            public void handleProgress(long l) {
                // print progress
            }
        });
        return ret;
    }

    protected Map<String, String> castTag(ObjectMetadata objectMetadata) {
        if (null == objectMetadata || !objectMetadata.hasTag()) {
            return null;
        }
        Collection<ObjectTagging.Tag> tagList = objectMetadata.getTagging().getTagSet();
        Map<String, String> tapMap = new HashMap<>();
        for (ObjectTagging.Tag tag : tagList) {
            tapMap.put(tag.getKey(), tag.getValue());
        }
        return tapMap;
    }

    protected <T> T callAzureMethod(String operation, AzureOperator<T> supplier) {
        try {
            return supplier.getResult();
        } catch (Exception ex) {
            throw new CloudException(operation + " failed", ex);
        }
    }

    private interface  AzureOperator<T> {
        T getResult() throws Exception;
    }
}
