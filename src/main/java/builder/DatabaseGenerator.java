package builder;

import com.mongodb.client.MongoDatabase;

public interface DatabaseGenerator {
  MongoDatabase generate();
  DatabaseGenerator setDocuments();
}
