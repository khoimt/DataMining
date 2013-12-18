/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package crawler;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Hieu Tran
 */
public class MongoDb {
    private MongoClient mongoClient;
    private DB db;
    private DBCollection collection;
    public MongoDb(String dbName, String collectionName)
    {
        try {
            mongoClient = new MongoClient("localhost");
            db = mongoClient.getDB(dbName);
            collection = db.getCollection(collectionName);
        } catch (UnknownHostException ex) {
            System.out.println("Cannot find MongoDb host");
            System.exit(1);
        }
    }
    
    public MongoDb(String dbName, String collectionName, String userName, char[] userPwd)
    {
        try {
            mongoClient = new MongoClient("localhost");
            db = mongoClient.getDB(dbName);
            boolean auth = db.authenticate(userName, userPwd);
            collection = db.getCollection(collectionName);
            
        } catch (UnknownHostException ex) {
            System.out.println("Cannot find MongoDb host");
            System.exit(1);
        }
    }
    
    public DBCollection getCollection(String collectionName){
        collection = db.getCollection(collectionName);
        return collection;
    }
    
    public void insertDocument(BasicDBObject dbob){
        collection.insert(dbob);
    }
    
    public void insertArticle(Article article ){
        BasicDBObject document = new BasicDBObject();
        document.put("Url", article.Url);
        document.put("Html", article.htmlContent);
        document.put("Title", article.Title);
        document.put("PubDate", article.pubDate);
        document.put("Content", article.Content);
        insertDocument(document);
    }
    
    public DBObject FindOne(){
        DBObject myDoc = collection.findOne();
        return myDoc;
    }
    
    public void dropDatabase(String databaseName){
        mongoClient.dropDatabase(databaseName);
    }
}
