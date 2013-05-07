
package org.fcrepo.serialization.bagit;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

public class BagItSerializerIT extends AbstractResourceIT {

    @Test
    public void tryOneObject() throws ClientProtocolException, IOException {
        client.execute(postObjMethod("BagIt1"));
        client.execute(postDSMethod("BagIt1", "testDS", "stuff"));
        final HttpGet getObjMethod =
                new HttpGet(serverAddress + "objects/BagIt1/fcr:export?format=bagit");
        HttpResponse response = client.execute(getObjMethod);
        assertEquals(200, response.getStatusLine().getStatusCode());
        final String content = EntityUtils.toString(response.getEntity());
        logger.debug("Found exported object: " + content);
        client.execute(new HttpDelete(serverAddress + "objects/BagIt1"));
        logger.debug("Deleted test object.");
        final HttpPost importMethod =
                new HttpPost(serverAddress + "objects/fcr:import?format=bagit");
        importMethod.setEntity(new StringEntity(content));
        assertEquals("Couldn't import!", 201, getStatus(importMethod));
        response =
                client.execute(new HttpGet(serverAddress + "objects/BagIt1"));
        assertEquals("Couldn't find reimported object!", 200, response
                .getStatusLine().getStatusCode());
        response =
                client.execute(new HttpGet(serverAddress +
                        "objects/BagIt1/testDS"));
        assertEquals("Couldn't find reimported datastream!", 200, response
                .getStatusLine().getStatusCode());
        logger.debug("Successfully reimported!");
    }
}
