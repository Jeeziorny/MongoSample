import builder.BookstoreGenerator;
import builder.FilmLibraryGenerator;
import builder.DatabaseGenerator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Thread.sleep;

public class MongoTest { //Director
  private DatabaseGenerator generator;
  private static MongoDatabase FilmLibraryDB;
  private static MongoDatabase BookstoreDB;

  MongoTest(final DatabaseGenerator gen) {
    generator = gen;
  }

  private MongoDatabase construct() {
    return generator.setDocuments().generate();
  }

  private void setGenerator(final DatabaseGenerator generator) {
    this.generator = generator;
  }

  public static void main(String[] args) {
    MongoClient mongoClient = new MongoClient();
    mongoClient.dropDatabase("MFilmLibrary");
    mongoClient.dropDatabase("MBookstore");
    DatabaseGenerator generator = new FilmLibraryGenerator(mongoClient);
    MongoTest director = new MongoTest(generator);
    FilmLibraryDB = director.construct();
    director.setGenerator(new BookstoreGenerator(mongoClient));
    BookstoreDB = director.construct();

    showCollectionInFilmLibrary();
    showFilmWithDirector();
    queryActors();


    try {
      sleep(2000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Exercise 6
   */
  private static void showCollectionInFilmLibrary() {
    MongoIterable<String> col = FilmLibraryDB.listCollectionNames();
    for (String s : col) {
      System.out.println(s);
    }
  }

  /**
   * Exercise 7
   */
  private static void showFilmWithDirector() {
    MongoCollection<Document> films = FilmLibraryDB.getCollection("films");
    FindIterable<Document> docs = films.find()
            .projection(include("title", "film_director"));
    for (Document doc : docs) {
      System.out.println(String.format("%-30s", doc.getString("title"))+" "+doc.getString("film_director"));
    }
  }

  /**
   * Exercise 8
   * SELECT * \ "_id"
   * FROM actors
   * WHERE (first_name = Hugh
   *        AND "australian" IN nationality)
   *        OR first_name = Al)
  */
  private static void queryActors() {
    MongoCollection<Document> actors = FilmLibraryDB
            .getCollection("actors");
    List<Document> documents = actors
            .find(eq("first_name", "Al"))
            .into(new ArrayList<Document>());
    List<Document> doc = actors.find()
            .into(new ArrayList<Document>());
    for (int i = 0; i < doc.size(); i++) {
      List<Document> nationalities = (ArrayList<Document>) doc.get(i).get("nationality");
      for (int j = 0; j < nationalities.size(); j++) {
        if (nationalities.get(j).get("Country").equals("Australia")
                && doc.get(i).get("first_name").equals("Hugh")) {
          documents.add(doc.get(i));
        }
      }
    }
    System.out.println(String.format("%-10s", "First") +
            String.format("%-10s", "Last") +
            String.format("%-10s", "Nation"));
    for (Document document : documents) {
      System.out.println(String.format("%-10s", document.getString("first_name")) + " "
              + String.format("%-10s", document.getString("last_name")) + " "
              + String.format("%-10s", document.get("nationality").toString()));
    }
  }

  /** Exercise 9
   * query for cast contains
   * more than 7 people
   * surpressing first result
   */
//  private static void queryFilms() {
//    MongoCollection<Document> films = FilmLibraryDB.getCollection("films");
//    DBObject query = new BasicDBObject("otherInfo.text", new BasicDBObject("$exists", true));
//    FindIterable result = films.find()
//  }
}