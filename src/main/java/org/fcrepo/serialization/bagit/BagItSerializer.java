
package org.fcrepo.serialization.bagit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.Files.createTempDir;
import static java.io.File.createTempFile;
import static org.fcrepo.utils.FedoraTypesUtils.isFedoraDatastream;
import static org.modeshape.jcr.api.JcrConstants.JCR_CONTENT;
import static org.modeshape.jcr.api.JcrConstants.JCR_DATA;
import static org.slf4j.LoggerFactory.getLogger;
import gov.loc.repository.bagit.Bag;
import gov.loc.repository.bagit.BagFactory;
import gov.loc.repository.bagit.BagFile;
import gov.loc.repository.bagit.BagInfoTxt;
import gov.loc.repository.bagit.utilities.SimpleResult;
import gov.loc.repository.bagit.utilities.namevalue.NameValueReader.NameValue;
import gov.loc.repository.bagit.writer.impl.ZipWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;

import org.fcrepo.Datastream;
import org.fcrepo.FedoraObject;
import org.fcrepo.exception.InvalidChecksumException;
import org.fcrepo.serialization.BaseFedoraObjectSerializer;
import org.fcrepo.utils.PropertyIterator;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

public class BagItSerializer extends BaseFedoraObjectSerializer {

    private Set<String> prefixes;

    private BagFactory bagFactory = new BagFactory();

    private Logger logger = getLogger(this.getClass());

    @Override
    public void serialize(final FedoraObject obj, final OutputStream out)
            throws RepositoryException, IOException {
        checkNotNull(obj, "Cannot serialize a null FedoraObject!");
        logger.debug("Serializing object: " + obj.getName());
        try (final InputStream is =
                new FileInputStream(serializeToFile(obj.getNode()))) {
            copy(is, out);
        }
    }

    public File serializeToFile(final Node node) throws RepositoryException,
            IOException {
        checkNotNull(node, "Cannot serialize a null Node!");
        final File bagFile = createTempDir();
        logger.debug("Bag assembly directory created at: {}", bagFile.getPath());
        // create bag-info.txt
        final File bagInfoTxtFile =
                new File(bagFile, bagFactory.getBagConstants().getBagInfoTxt());
        if (!bagInfoTxtFile.createNewFile()) {
            throw new IllegalStateException(
                    "Could not create a bag-info-.txt file!");
        }
        logger.debug("bag-info.txt file created at: {}", bagInfoTxtFile
                .getPath());
        bagInfoTxtFile.deleteOnExit();
        bagFile.deleteOnExit();
        final Bag bag = bagFactory.createBag();
        bag.addFileAsTag(bagInfoTxtFile);
        // get recordable properties
        logger.trace("Retrieving properties to serialize...");
        final Iterator<Property> properties = node.getProperties(prefixesInGlobForm());
        // put 'em in a tag info file
        logger.trace("Recording properties...");
        final BagInfoTxt bagInfoTxt = bag.getBagInfoTxt();
        // slip in object name
        bagInfoTxt.put("Name", node.getName());
        for (final Iterator<List<NameValue>> i =
                transform(properties, property2BagItTags); i.hasNext();) {
            for (final NameValue tag : i.next()) {
                logger.debug("Recording property: " + tag);
                bagInfoTxt.putList(tag);
            }
        }

        logger.trace("Recorded properties.");

        // now add content, if it exists
        if (node.hasNode(JCR_CONTENT)) {
            logger.trace("Recording binary content...");
            final File contentFile = new File(bagFile, "content");
            contentFile.deleteOnExit();
            try (final OutputStream contentFileStream =
                    new FileOutputStream(contentFile);
                    final InputStream contentStream =
                            node.getNode(JCR_CONTENT).getProperty(JCR_DATA)
                                    .getBinary().getStream()) {
                copy(contentStream, contentFileStream);
            }
            bag.addFileToPayload(contentFile);
            logger.trace("Recorded binary content.");
        }

        // and recurse, to pick up datastream children
        for (final Iterator i =
                filter(node.getNodes(), isFedoraDatastream); i
                .hasNext();) {
            final Node dsNode = (Node)i.next();
            logger.debug("Now recording child node: " + dsNode.getName());
            bag.addFileToPayload(serializeToFile(dsNode));
        }

        // create the final ZIP'd artifact
        logger.trace("Creating final ZIP artifact...");
        // we use a temp directory but a normal file inside it. this
        // is so we can control the name of the file, so that the names
        // of files in data/ in the bag are reasonable.
        final File housingDir = createTempDir();
        housingDir.deleteOnExit();
        final File nodeZippedBag = new File(housingDir, node.getName());
        nodeZippedBag.deleteOnExit();
        final Bag finishedBag = bag.makeComplete();
        final SimpleResult result = finishedBag.verifyComplete();
        for (final String errorMsg : result.getErrorMessages()) {
            throw new IllegalStateException(errorMsg);
        }
        finishedBag.write(new ZipWriter(bagFactory), nodeZippedBag);
        logger.trace("Created final ZIP artifact.");

        return nodeZippedBag;

    }

    @Override
    public void deserialize(final Session session, final String path, final InputStream stream)
            throws IOException, RepositoryException, InvalidChecksumException {
        logger.trace("Deserializing a Fedora object from a BagIt bag.");

        final File importFile = createTempFile("fedora-bagit-import", "");
        importFile.deleteOnExit();
        try (final OutputStream importStream = new FileOutputStream(importFile)) {
            copy(stream, importStream);
        }

        final Bag bag = bagFactory.createBag(importFile);
        logger.trace("Created temporary Bag for Fedora object.");
        final BagInfoTxt infoTxt = bag.getBagInfoTxt();

		final String objectPath = path + "/" + infoTxt.get("Name");

        // first make object and add its properties
        final FedoraObject object =
                objService.createObject(session, objectPath);
        logger.debug("Created Fedora object: " + objectPath);
        for (final String key : filter(infoTxt.keySet(), notNameAndIsPrefixed)) {
            final List<String> values = infoTxt.getList(key);
            logger.debug("Adding property for: " + key + " with values: " +
                    values);
            if (values.size() == 1) {
                object.getNode().setProperty(key.replace('_', ':'),
                        values.get(0));
            } else {
                object.getNode().setProperty(key.replace('_', ':'),
                        infoTxt.getList(key).toArray(new String[0]));
            }
        }

        // now for its datastreams
        for (final BagFile bagFile : bag.getPayload()) {
            logger.debug("Deserializing a datastream from filepath: " +
                    bagFile.getFilepath());
            final File importDsFile =
                    createTempFile("fedora-bagit-import-ds", "");
            importDsFile.deleteOnExit();
            try (final InputStream dsStream = bagFile.newInputStream();
                    final OutputStream out = new FileOutputStream(importDsFile)) {
                copy(dsStream, out);
            }
            final Bag dsBag = bagFactory.createBag(importDsFile);
            logger.trace("Created temporary Bag file for datastream.");
            final BagInfoTxt dsInfoTxt = dsBag.getBagInfoTxt();
            final String dsPath = objectPath + "/" + dsInfoTxt.get("Name");
            logger.debug("Found ds path: " + dsPath);
            final String contentType = dsInfoTxt.get("fedora_contentType");
            logger.debug("Found ds contentType: " + contentType);
            final InputStream requestBodyStream =
                    dsBag.getPayload().iterator().next().newInputStream();
            final Datastream ds =
                    new Datastream(dsService.createDatastreamNode(session,
                            dsPath, contentType, requestBodyStream));
            logger.debug("Created Fedora datastream: " + ds.getDsId());
        }

        session.save();

    }

    private Function<Property, List<NameValue>> property2BagItTags =
            new Function<Property, List<NameValue>>() {

                @Override
                public List<NameValue> apply(final Property p) {
                    try {
                        final String name = p.getName().replace(':', '_');
                        if (p.isMultiple()) {
                            return Lists.transform(copyOf(p.getValues()),
                                    new Function<Value, NameValue>() {

                                        @Override
                                        public NameValue apply(final Value v) {
                                            try {
                                                return new NameValue(name, v
                                                        .getString());
                                            } catch (final RepositoryException e) {
                                                throw new IllegalStateException(
                                                        e);
                                            }
                                        }
                                    });

                        } else {
                            return of(new NameValue(name, p.getString()));
                        }

                    } catch (final RepositoryException e) {
                        throw new IllegalStateException(e);
                    }
                }
            };

    private Predicate<String> notNameAndIsPrefixed = new Predicate<String>() {

        @Override
        public boolean apply(final String key) {
            return !key.equals("Name") && prefixes.contains(key.split("_")[0]);
        }
    };

    public void setPrefixes(final Set<String> prefixes) {
        this.prefixes = prefixes;
    }

    private String[] prefixesInGlobForm() {
        return transform(prefixes, makeGlob).toArray(new String[0]);
    }

    private Function<String, String> makeGlob = new Function<String, String>() {

        @Override
        public String apply(final String input) {
            // TODO Auto-generated method stub
            return input + ":*";
        }
    };

}
