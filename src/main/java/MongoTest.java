import builder.DatabaseGenerator;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;

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
//    mongoClient.dropDatabase("MFilmLibrary");
//    mongoClient.dropDatabase("MBookstore");
//    DatabaseGenerator generator = new FilmLibraryGenerator(mongoClient);
//    MongoTest director = new MongoTest(generator);
    FilmLibraryDB = mongoClient.getDatabase("MFilmLibrary");
//    director.setGenerator(new BookstoreGenerator(mongoClient));
    BookstoreDB = mongoClient.getDatabase("MBookstore");

    showCollectionInFilmLibrary();
    showFilmWithDirector();
    queryActors();
    queryFilms();
    harrisMovie();
    dropActors();
    deleteFederalInformation();


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
   * SELECT first_name, last_name, nationality
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
  private static void queryFilms() {
    MongoCollection<Document> films = FilmLibraryDB.getCollection("films");
    FindIterable<Document> result = films.find(eq("cast.8", new Document("$exists", true)));
    for (Document document : result) {
      System.out.println(document.get("title"));
    }
  }

  /** Exercise 10
   * For each actor with last_name == "Harris"
   * print first name, last name and titles
   * where he or she played.
  */
  private static void harrisMovie() {
    MongoCollection actors = FilmLibraryDB.getCollection("actors");
    Bson LastNameLookup = new Document("$lookup",
                    new Document("from", "films"  )
                    .append("localField", "last_name")
                    .append("foreignField", "cast.actor_last_name")
                    .append("as", "movies"));
    Bson FirstNameLookup = new Document("$lookup",
            new Document("from", "films"  )
                    .append("localField", "first_name")
                    .append("foreignField", "cast.actor_name")
                    .append("as", "movies"));
    Bson LastNameMatch = new Document("last_name", "Harris");
    AggregateIterable<Document> temp = actors
            .aggregate(asList(LastNameLookup, FirstNameLookup,
                    Aggregates.match(LastNameMatch)));
    for (Document document : temp) {
      System.out.println(document.get("first_name")+" "
              +document.get("last_name")+" "
              +document.get("nationality"));
      List<Document> titles = (ArrayList<Document>) document.get("movies");
      for (Document film : titles) {
        System.out.print(film.get("title")+", ");
      }
      System.out.println("");
    }
  }

  /** Exercise 11
   * delete actors from actors collection,
   * who has ['comedy', 'criminal'] in
   * typical roles
   */
  private static void dropActors() {
    MongoCollection<Document> actors = FilmLibraryDB.getCollection("actors");
    DeleteResult result = actors.deleteMany(eq("typical_role", asList("Criminal", "Comedy")));
    System.out.println("deleted: "+result.getDeletedCount());
  }

  /** Exercise 12
   * Replaces field STATE in nationality array
   * if Country == Russia to "Russia"
   */
  private static void deleteFederalInformation() {
    MongoCollection<Document> actors = FilmLibraryDB
            .getCollection("actors");
    Document query = new Document();
    query.put("nationality.Country", "Russia");
    Document data = new Document();
    data.put("nationality.$.State", "Russia");
    Document command = new Document();
    command.put("$set", data);
    actors.updateMany(query, command);
  }
}