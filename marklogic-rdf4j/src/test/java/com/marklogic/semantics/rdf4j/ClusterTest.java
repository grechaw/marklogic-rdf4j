package com.marklogic.semantics.rdf4j;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ClusterTest extends Rdf4jTestBase {
    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Before
    public void setUp()
            throws Exception {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn = rep.getConnection();
        logger.info("test setup complete.");
    }

    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        if (conn.isOpen() && conn.isActive()) {
            conn.rollback();
        }
        //conn.clear();
        if (conn.isOpen()) {
            conn.clear();
        }
        conn.close();
        rep.shutDown();
        conn = null;
        logger.info("tearDown complete.");
    }

    @Test
    public void testAddNQuadsOrTrigs() throws Exception {
        File folder = new File("/Users/asonvane/Desktop/data2/");
        File[] listOfFiles = folder.listFiles();
        long startTime;

        for (int i = 0; i < listOfFiles.length; i++) {
            System.out.println("Name: " + listOfFiles[i].getName());

            String ext = listOfFiles[i].toString().substring(listOfFiles[i].toString().lastIndexOf(".") + 1);
            if (ext.equals("trig")) {
                startTime = System.currentTimeMillis();
                System.out.println("Start time: " + startTime);

                conn.begin();
                conn.add(listOfFiles[i], "http://example.org/example1/", RDFFormat.TRIG);
                conn.commit();

                System.out.println("Ingestion time: " + (System.currentTimeMillis() - startTime));
            } else if (ext.equals("nquad") || ext.equals("nq")) {
                startTime = System.currentTimeMillis();
                System.out.println("Start time: " + startTime);

                conn.begin();
                conn.add(listOfFiles[i], "http://example.org/example1/", RDFFormat.NQUADS);
                conn.commit();

                System.out.println("Ingestion time: " + (System.currentTimeMillis() - startTime));
            } else {
                System.out.println("Wrong format");
            }

            clearDB();
            System.out.println("=======================================================");
        }
    }

    @Test
    public void clearDB() {
        DefaultHttpClient client = null;
        try {
            client = new DefaultHttpClient();
            client.getCredentialsProvider().setCredentials(
                    new AuthScope(host, port),
                    new UsernamePasswordCredentials("admin", "admin"));
            String uri = "http://" + host + ":" + port + "/v1/search/";
            HttpDelete delete = new HttpDelete(uri);
            client.execute(delete);

        } catch (Exception e) {
            // writing error to Log
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
