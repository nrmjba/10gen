package org.lenition.mj101;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StudentITest {

    MongoClient client;
    DB testDB;
    DBCollection testCollection;

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws IOException {
        client = new MongoClient();
        // aka MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        testDB = client.getDB("school");
        testCollection = testDB.getCollection("students");
        testCollection.drop();
        InputStream is = StudentITest.class.getResourceAsStream("/students.432aefc2cf4e.json");
        assertNotNull(is);
        assertTrue(is.available() > 0);
        String json = convertStreamToString(is);
        for(String line : json.split("\n")) {
            DBObject student = (DBObject) JSON.parse(line);
            testCollection.insert(student);
        }
    }

    @After
    public void tearDown() {
        testCollection.drop();
        testDB.dropDatabase();
        testDB = null;
    }

    @Test
    public void dropLowestGrade() {
        assertEquals(200, testCollection.count());
    }

    private static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
