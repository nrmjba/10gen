package org.lenition.mj101;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

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

        assertEquals(200, testCollection.count());
    }

    @After
    public void tearDown() {
        testCollection.drop();
        testDB.dropDatabase();
        testDB = null;
    }

    @Test
    public void dropLowestHomeworkGrade() {
        assertEquals(200, testCollection.count());
        DBCursor cursor = testCollection.find();
        while (cursor.hasNext()) {
            DBObject student = cursor.next();
            BasicDBList grades = (BasicDBList)student.get("scores");
            assertTrue(grades.size() > 0);
            Object lowestHomeworkGrade = null;
            for(Object grade : grades) {
                if (((DBObject)grade).get("type").equals("homework")) {
                    if(lowestHomeworkGrade == null || (((BasicDBObject)lowestHomeworkGrade).getDouble("score") > ((BasicDBObject)grade).getDouble("score"))) {
                        lowestHomeworkGrade = grade;
                    }
                }
            }
            grades.remove(lowestHomeworkGrade);
            assertTrue(grades.size() == 3);
            testCollection.update(student, new BasicDBObject("$set", new BasicDBObject("scores", grades)));
            testCollection.save(student);
        }

        AggregationOutput out = testCollection.aggregate(   (DBObject)JSON.parse("{'$unwind':'$scores'}"),
                (DBObject)JSON.parse("{'$group':{'_id':'$_id', 'average':{$avg:'$scores.score'}}}"),
                (DBObject)JSON.parse("{'$sort':{'average':-1}}, {'$limit':1}"));

        CommandResult result = out.getCommandResult();
        System.out.println(result);
    }


    private void printGrades(Collection<Object> grades) {
        for(Object grade : grades) {
            System.out.print("  " + ((BasicDBObject)grade).get("type") + ":" + ((BasicDBObject)grade).get("score"));
        }
        System.out.println();
    }

    private static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
