import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

public class EmbeddedNeo4jWithIndexing {
	private static final String DB_PATH = "/home/mrhooray/Documents/neo4j_db/";
	private static final String USERNAME_KEY = "username";
	private static GraphDatabaseService graphDb;
	private static Index<Node> nodeIndex;

	// START SNIPPET: createRelTypes
	private static enum RelTypes implements RelationshipType {
		USERS_REFERENCE, USER
	}

	// END SNIPPET: createRelTypes

	public static void main(final String[] args) {
		System.out.println(Runtime.getRuntime().maxMemory()/1024/1024);
		// START SNIPPET: startDb
		graphDb = new EmbeddedGraphDatabase(DB_PATH);
		nodeIndex = graphDb.index().forNodes("nodes");
		registerShutdownHook();
		// END SNIPPET: startDb

		// START SNIPPET: addUsers
		Transaction tx = graphDb.beginTx();
		Node userNode = null;
		Node usersReferenceNode = null;
		if(graphDb.getReferenceNode().hasRelationship(RelTypes.USERS_REFERENCE)){
			Iterator<Relationship> itr=graphDb.getReferenceNode().getRelationships(RelTypes.USERS_REFERENCE).iterator();
			usersReferenceNode=itr.next().getEndNode();
		}else{
			usersReferenceNode=graphDb.createNode();
			graphDb.getReferenceNode().createRelationshipTo(usersReferenceNode,RelTypes.USERS_REFERENCE);
		}
		printDBSize();
		printRelationSize();
		tx.success();
		tx.finish();
		tx=null;
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			System.out.println(Runtime.getRuntime().totalMemory()/1024/1024);
			tx = graphDb.beginTx();
			for (int id = 0; id < 10; id++) {
				userNode = createAndIndexUser(idToUserName(id));
				usersReferenceNode
						.createRelationshipTo(userNode, RelTypes.USER);
				userNode = null;
			}
			tx.success();
			tx.finish();
			tx=null;

		}
//		tx = graphDb.beginTx();
//		Node Node1 = createAndIndexUser(idToUserName(1));
//		Node Node2 = createAndIndexUser(idToUserName(2));
//		Node node1=nodeIndex.get(USERNAME_KEY, "user" + 1 + "@neo4j.org").getSingle();
//		Node node2=nodeIndex.get(USERNAME_KEY, "user" + 2 + "@neo4j.org").getSingle();
//		node1.createRelationshipTo(node2, RelTypes.USER);
//		tx.success();
//		tx.finish();
//		tx=null;
//		tx.success();
//		tx.finish();
		long t2 = System.currentTimeMillis();
		System.out.println("It took " + (t2 - t1) + " ms");
		shutdown();
	}

	private static void shutdown() {
		graphDb.shutdown();
	}

	// START SNIPPET: helperMethods
	private static String idToUserName(final int id) {
		return "user" + id + "@neo4j.org";
	}

	private static Node createAndIndexUser(final String username) {
		Node node = graphDb.createNode();
		node.setProperty(USERNAME_KEY, username);
		nodeIndex.add(node, USERNAME_KEY, username);
		return node;
	}

	// END SNIPPET: helperMethods

	private static void registerShutdownHook() {
		// Registers a shutdown hook for the Neo4j and index service instances
		// so that it shuts down nicely when the VM exits (even if you
		// "Ctrl-C" the running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shutdown();
			}
		});
	}
	
	public static void printDBSize() {
		GlobalGraphOperations g = GlobalGraphOperations.at(graphDb);
		Iterator<Node> itr = g.getAllNodes().iterator();
		int count = 0;
		while (itr.hasNext()) {
			itr.next();
			count++;
		}
		System.out.println("Size of Graph DB: " + count);
	}

	public static void printRelationSize() {
		GlobalGraphOperations g = GlobalGraphOperations.at(graphDb);
		Iterator<Relationship> itr = g.getAllRelationships().iterator();
		int count = 0;
		while (itr.hasNext()) {
			itr.next();
			count++;
		}
		System.out.println("Size of Relationships: " + count);
	}
}