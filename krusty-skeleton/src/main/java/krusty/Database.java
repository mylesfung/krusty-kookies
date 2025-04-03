package krusty;

import spark.Request;
import spark.Response;

import java.sql.*;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static krusty.Jsonizer.toJson;

public class Database {
	/**
	 * Modify it to fit your environment and then use this string when connecting to your database!
	 */
	public void connect() {
		try {
			connection = DriverManager.getConnection(jdbcString, jdbcUsername, jdbcPassword);
			System.out.println("Connected to the database successfully!");
		} catch (SQLException e) {
			System.err.println("Failed to connect to the database.");
			e.printStackTrace();
		}
	}

	// TODO: Implement and change output in all methods below!

	// For use with MySQL or PostgreSQL
	private static final String jdbcUsername = "hbg43";
	private static final String jdbcPassword = "gzr549sb";
	private static final String jdbcString = "jdbc:mysql://puccini.cs.lth.se/" + jdbcUsername;

	private Connection connection;

	// TODO: Implement and change output in all methods below!

	public String getCustomers(Request req, Response res) {
		String query = "SELECT name, adress FROM Customer";
		try (PreparedStatement stmt = connection.prepareStatement(query);
			 ResultSet rs = stmt.executeQuery()) {

			return Jsonizer.toJson(rs, "customers");
		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String getRawMaterials(Request req, Response res) {
		String query = "SELECT ingredient AS name, amount, unit FROM Storage;";
		try (PreparedStatement stmt = connection.prepareStatement(query);
			 ResultSet rs = stmt.executeQuery()) {

			return Jsonizer.toJson(rs, "raw-materials");

		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String getCookies(Request req, Response res) {
		String query = "SELECT cookie_name AS name FROM Recipes";
		try(PreparedStatement stmt = connection.prepareStatement(query);
			ResultSet rs = stmt.executeQuery()) {
			return Jsonizer.toJson(rs, "cookies");
		} catch (SQLException e) {
			e.printStackTrace();
			return "{\"cookies\":[]}";
		}

	}

	public String getRecipes(Request req, Response res) {
		String query = """
            SELECT R.cookie_name AS cookie, S.ingredient AS raw_material, I.amount, S.unit 
            FROM Recipes R, Storage S, Ingredients I
            WHERE R.ID = I.recipe_id
            AND I.storage_id = S.ID
            ORDER BY cookie ASC;
        """;
		try (PreparedStatement stmt = connection.prepareStatement(query);
			 ResultSet rs = stmt.executeQuery()) {

			return Jsonizer.toJson(rs, "recipes");

		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String getPallets(Request req, Response res) {
		return "{\"pallets\":[]}";
	}

	public String reset(Request req, Response res) {
		return "{}";
	}

	public String createPallet(Request req, Response res) {
		return "{}";
	}
}
