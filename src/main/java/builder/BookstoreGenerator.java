package builder;

import builder.DataGenerator.ConcreteGenerator;
import builder.DataGenerator.DataGenerator;
import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import model.Person;
import model.Title;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class BookstoreGenerator implements DatabaseGenerator {
  private MongoDatabase MBookstore;
  private Random randGenerator = new Random();
  private DataGenerator dataGenerator;

  public BookstoreGenerator(final MongoClient mongoClient) {
    dataGenerator = new ConcreteGenerator();
    this.MBookstore = mongoClient
            .getDatabase("MBookstore");
    MBookstore.getCollection("books");
  }

  public MongoDatabase generate() {
    return this.MBookstore;
  }

  public DatabaseGenerator setDocuments() {
    insertBooks();
    insertBooksForQuering();
    return this;
  }

  private void insertBooks() {
    MongoCollection<Document> filmsCollection = MBookstore.getCollection("books");
    Person[] authors = dataGenerator.getPersons();
    Title[] titles = dataGenerator.getTitles();
    for (int i = 0; i < 15; i++) {
      filmsCollection.insertOne(new Document("author", authors[i].getFirst_name()+" "+authors[i].getLast_name())
                                      .append("title", titles[i].getTitle())
                                      .append("release_date", dataGenerator.getDate())
                                      .append("main_characters", getMainCharachters()));
    }
  }

  private ArrayList<String> getMainCharachters() {
    Person[] characters = dataGenerator.getPersons();
    ArrayList<String> result = new ArrayList<String>();
    final int bound = randGenerator.nextInt(5)+2;
    for (int i = 0; i < bound; i++) {
      result.add(characters[randGenerator.nextInt(20)+25].getFirst_name());
    }
    return result;
  }

  private void insertBooksForQuering() {
    ArrayList<String> main_char =
            new ArrayList<String>();
    main_char.addAll(Arrays.asList("rectangle", "square"));
    MBookstore.getCollection("books").insertOne(new Document("author", "Steve")
                          .append("title", "CIRCLE")
                          .append("release_date", "2000")
                          .append("main_characters", main_char));
  }
}
