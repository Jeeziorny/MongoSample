import builder.BookstoreGenerator;
import builder.DatabaseGenerator;
import builder.FilmLibraryGenerator;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;
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
    mongoClient.dropDatabase("MFilmLibrary");
    mongoClient.dropDatabase("MBookstore");
    DatabaseGenerator generator = new FilmLibraryGenerator(mongoClient);
    MongoTest director = new MongoTest(generator);
    FilmLibraryDB = director.construct();
    FilmLibraryDB = mongoClient.getDatabase("MFilmLibrary");
    director.setGenerator(new BookstoreGenerator(mongoClient));
    BookstoreDB = director.construct();
    BookstoreDB = mongoClient.getDatabase("MBookstore");

//    showCollectionInFilmLibrary();
//    showFilmWithDirector();
//    queryActors();
//    queryFilms();
//    harrisMovie();
//    dropActors();
//    deleteFederalInformation();
//    sortActorByAge();
//    listAgents();
//    listTitles();
//    titleAuthorDirector(true);
    bookAdaptation();

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

  /** Exercise 13
   * lists all actors ordered by age (ascending)
   */
  private static void sortActorByAge() {
    MongoCollection actors = FilmLibraryDB.getCollection("actors");
    ArrayList<Document> doc = (ArrayList<Document>) actors.find()
            .sort(descending("date_of_birth"))
            .into(new ArrayList<Document>());
    for (Document document : doc) {
      System.out.println(document.get("first_name")+" "
              +document.get("last_name")+" "+document.get("date_of_birth"));
    }


  }

  /** Exercise 14
   * Query agents who have no V, X and Q
   * letter in last name
   * AND
   * have no V, X and Q in first name
   * except this ones who's name is "KEVIN".
   */
  private static void listAgents() {
    MongoCollection<Document> agents = FilmLibraryDB
            .getCollection("agents");
    FindIterable<Document> result = agents
            .find(
            and(
                    regex("last_name", Pattern.compile("\\b[^VXQ]+\\b")),
                    or(
                            regex("first_name", Pattern.compile("\\b[^VXQ]+\\b")),
                            regex("first_name", Pattern.compile("(KEVIN)"))
                    )
                    ,(eq("corporation", new Document("$exists", true))) //comment this line for query also agents with no corpo;
            ));
    System.out.println("For each agent with no other condition: ");
    for (Document doco : result) {
      System.out.println("first_name: "+doco.get("first_name")+
                         ", last_name: "+doco.get("last_name"));
    }
  }

  /** Exercise 15
   * lists all title from BookstoreDB
   */
  private static void listTitles() {
    FindIterable<Document> books =
            BookstoreDB.getCollection("books").find();
    for (Document doc : books) {
      System.out.println(doc.get("title"));
    }
  }

  /**
   * Exercise 16
   * List title of book, author of book
   * and film_director of film whith
   * the same title as book.
   *
   * Problem: $lookup is only available
   * on collections in the same db
   *
   * @param print - when true its exercise 16,
   *                   when false its part of exercise 17.
   */
  private static ArrayList<Document> titleAuthorDirector(boolean print) {
    FindIterable<Document> films =
            FilmLibraryDB.getCollection("films").find();
    FindIterable<Document> books =
            BookstoreDB.getCollection("books").find();
    System.out.println(String.format("%-25s", "Title")+
            String.format("%-25s", "Book author") +
            String.format("%-25s", "Film director"));
    ArrayList<Document> resultOfEx17 = new ArrayList<Document>();
    for (Document book : books) {
      for (Document film : films) {
        if (book.get("title").equals(film.get("title")) && print) {
          System.out.println(String.format("%-25s", book.get("title"))+
                             String.format("%-25s", book.get("author")) +
                             String.format("%-25s", film.get("film_director")));
        }
      }
      if (!print) {
        resultOfEx17.add(book);
      }
    }
    return resultOfEx17;
  }

  /** Exercise 17 */
  private static void bookAdaptation() {
    ArrayList<Document> keyBooks =
            titleAuthorDirector(false);
    MongoCollection<Document> films =
            FilmLibraryDB.getCollection("films");
    Document result;
    System.out.println(String.format("%-25s", "TYTUL FLMU")+
            String.format("%-25s", "DATA FILMU")+
            String.format("%-25s", "DATA KSIAZKI"));
    for (Document book : keyBooks) {
      result = films.find(
              and(
                      eq("title", book.get("title")),
                      gt("release_date", book.get("release_date"))
              )
      ).first();

      if (result != null && checkCharacters(result, book)) {
        System.out.println(String.format("%-25s", result.get("title"))+
                String.format("%-25s", result.get("release_date"))+
                String.format("%-25s", book.get("release_date")));


      }
    }
  }

  private static boolean checkCharacters(Document film, Document book) {
    ArrayList<String> mainCharacters = ((ArrayList) book.get("main_characters"));
    ArrayList<Document> actors = (ArrayList)film.get("cast");
    ArrayList<String> roles_in_films = new ArrayList<String>();
    for (Document actor : actors) {
      roles_in_films.add((String)actor.get("role_in_film"));
    }
    for (String name : mainCharacters) {
      if (!roles_in_films.contains(name)) {
        return false;
      }
    }
    return true;
  }
}