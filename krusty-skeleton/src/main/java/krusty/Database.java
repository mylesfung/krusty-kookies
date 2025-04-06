package krusty;

import spark.Request;
import spark.Response;

import java.sql.*;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;

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
		String query = "SELECT name, address FROM Customers ORDER BY name";
		try (PreparedStatement stmt = connection.prepareStatement(query);
			 ResultSet rs = stmt.executeQuery()) {

			return Jsonizer.toJson(rs, "customers");
		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String getRawMaterials(Request req, Response res) {
		String query = "SELECT ingredient AS name, amount, unit FROM Storage ORDER BY name";
		try (PreparedStatement stmt = connection.prepareStatement(query);
			 ResultSet rs = stmt.executeQuery()) {

			return Jsonizer.toJson(rs, "raw-materials");

		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String getCookies(Request req, Response res) {
		String query = "SELECT cookie_name AS name FROM Recipes ORDER BY name";
		try(PreparedStatement stmt = connection.prepareStatement(query);
			ResultSet rs = stmt.executeQuery()) {
			return Jsonizer.toJson(rs, "cookies");
		} catch (SQLException e) {
			e.printStackTrace();
			return "{\"cookies\":[]}";
		}

	}

	public String getRecipes(Request req, Response res) {
		String query =  "SELECT R.cookie_name AS cookie, S.ingredient AS raw_material, I.amount, S.unit" +
            			"FROM Recipes R, Storage S, Ingredients I" +
						"WHERE R.ID = I.recipe_id" +
           				"AND I.storage_id = S.ID" +
            			"ORDER BY cookie;";

		try (PreparedStatement stmt = connection.prepareStatement(query);
			 ResultSet rs = stmt.executeQuery()) {

			return Jsonizer.toJson(rs, "recipes");

		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String getPallets(Request req, Response res) {
		String sql = "SELECT id, cookie, production_datetime, customer, IF(is_blocked, 'yes', 'no') AS blocked FROM Pallets";
		ArrayList<String> conditions = new ArrayList<>();
		ArrayList<String> values = new ArrayList<>();

		if (req.queryParams("from") != null) {
			conditions.add("production_datetime >= ?");
			values.add(req.queryParams("from"));
		}
		if (req.queryParams("to") != null) {
			conditions.add("production_datetime <= ?");
			values.add(req.queryParams("to"));
		}
		if (req.queryParams("cookie") != null) {
			conditions.add("cookie = ?");
			values.add(req.queryParams("cookie"));
		}
		if (req.queryParams("blocked") != null) {
			conditions.add("is_blocked = ?");
			values.add(req.queryParams("blocked").equals("yes") ? "TRUE" : "FALSE");
		}
		if (!conditions.isEmpty()) {
			sql += " WHERE " + String.join(" AND ", conditions);
		}
		sql += " ORDER BY production_datetime DESC";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {

			for (int i = 0; i < values.size(); i++) {
				stmt.setString(i + 1, values.get(i));
			}
			try (ResultSet rs = stmt.executeQuery()){
				return Jsonizer.toJson(rs, "pallets");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return "{\"pallets\":[]}";
		}
	}

	public String reset(Request req, Response res) {

		try (Statement stmt = connection.createStatement()) {

			stmt.execute("SET FOREIGN_KEY_CHECKS = 0;");
			stmt.execute("TRUNCATE TABLE Customers;");
			stmt.execute("TRUNCATE TABLE Recipes;");
			stmt.execute("TRUNCATE TABLE Storage;");
			stmt.execute("TRUNCATE TABLE Ingredients;");
			stmt.execute("TRUNCATE TABLE Orders;");
			stmt.execute("TRUNCATE TABLE Pallets;");
			stmt.execute("TRUNCATE TABLE Amount;");
			stmt.execute("SET FOREIGN_KEY_CHECKS = 1;");

			stmt.execute(
                "INSERT INTO Customers (name, address) VALUES " +
                "('Bjudkakor AB', 'Ystad'), " +
                "('Finkakor AB', 'Helsingborg'), " +
                "('Gästkakor AB', 'Hässleholm'), " +
                "('Kaffebröd AB', 'Landskrona'), " +
                "('Kalaskakor AB', 'Trelleborg'), " +
                "('Partykakor AB', 'Kristianstad'), " +
                "('Skånekakor AB', 'Perstorp'), " +
                "('Småbröd AB', 'Malmö');"
            );

			stmt.execute(
                "INSERT INTO Recipes (cookie_name) VALUES " +
                "('Almond delight'), ('Amneris'), ('Berliner'), ('Nut cookie'), ('Nut ring'), ('Tango');"
            );

			stmt.execute(
                "INSERT INTO Storage (ingredient, amount, unit) VALUES " +
                "('Bread crumbs', 500000, 'g'), " +
                "('Butter', 500000, 'g'), " +
                "('Chocolate', 500000, 'g'), " +
                "('Chopped almonds', 500000, 'g'), " +
                "('Cinnamon', 500000, 'g'), " +
                "('Egg whites', 500000, 'ml'), " +
                "('Eggs', 500000, 'g'), " +
                "('Fine-ground nuts', 500000, 'g'), " +
                "('Flour', 500000, 'g'), " +
                "('Ground, roasted nuts', 500000, 'g'), " +
                "('Icing sugar', 500000, 'g'), " +
                "('Marzipan', 500000, 'g'), " +
                "('Potato starch', 500000, 'g'), " +
                "('Roasted, chopped nuts', 500000, 'g'), " +
                "('Sodium bicarbonate', 500000, 'g'), " +
                "('Sugar', 500000, 'g'), " +
                "('Vanilla sugar', 500000, 'g'), " +
                "('Vanilla', 500000, 'g'), " +
                "('Wheat flour', 500000, 'g');"
            );

			stmt.execute(
					"INSERT INTO Ingredients (recipe_id, storage_id, amount) VALUES " +
							// Almond delight
							"(1, 2, 400), (1, 4, 279), (1, 5, 10), (1, 9, 400), (1, 16, 270), " +
							// Amneris
							"(2, 2, 250), (2, 7, 250), (2, 12, 750), (2, 13, 25), (2, 19, 25), " +
							// Berliner
							"(3, 2, 250), (3, 3, 50), (3, 7, 50), (3, 9, 350), (3, 11, 100), (3, 17, 5), " +
							// Nut cookie
							"(4, 1, 125), (4, 3, 50), (4, 6, 350), (4, 8, 750), (4, 10, 625), (4, 16, 375), " +
							// Nut ring
							"(5, 2, 450), (5, 9, 450), (5, 11, 190), (5, 14, 225), " +
							// Tango
							"(6, 2, 200), (6, 9, 300), (6, 15, 4), (6, 16, 250), (6, 18, 2);"
			);
			return "{\"status\": \"ok\"}";

		} catch (SQLException e) {
			e.printStackTrace();
			return "{}";
		}
	}

	public String createPallet(Request req, Response res) {
		// Creates a new pallet, where the cookie is specified with the query parameter cookie
		//
		// 1. create new pallet, using req.queryParams and NOW()
		// 2. return pallet_id
		// 3. update storage based on recipe

		String cookie;
		if (req.queryParams("cookie") != null) {
			cookie = req.queryParams("cookie");
		} else {
			System.out.println("ERROR: No cookie specified in request");
			return "";
		}

		int newPalletID = 0;
		int recipeID;
		String checkCookieSQL = "select * from Recipes where cookie_name = ?";
		try (PreparedStatement ps = connection.prepareStatement(checkCookieSQL)) {
			ps.setString(1, cookie);
			ResultSet recipes = ps.executeQuery();
			if (!recipes.next()) {
				// Cookie name does not exist in Recipes
				return "{\"status\": \"error\"}";
			} else {
				recipeID = recipes.getInt("ID");

				// Create new pallet
				String insertPalletSQL = "insert into Pallets (production_datetime, location, recipe_id) " +
						"values(NOW(), 'Krusty Factory', ?)";
				try (PreparedStatement stmt = connection.prepareStatement(insertPalletSQL, Statement.RETURN_GENERATED_KEYS)) {
					stmt.setInt(1, recipeID);
					stmt.executeUpdate();
					// Set new pallet ID
					ResultSet key = stmt.getGeneratedKeys();
					if (key.next()) {
						newPalletID = key.getInt(1);
					}
				} catch (SQLException e) {
					e.printStackTrace();
					return "{\"status\": \"error\"}";
				}
			}
		} catch (SQLException e) {
			System.out.println("SQL EXCEPTION: " + e.getMessage());
			return "{\"status\": \"error\"}";
		}

		// Update storage
		String ingredientSQL = "select * from Ingredients where recipe_id = ?";
		try (PreparedStatement ps = connection.prepareStatement(ingredientSQL)) {
			ps.setInt(1, recipeID);
			ResultSet ingredients = ps.executeQuery();
			while (ingredients.next()) {
				int amt = ingredients.getInt("amount");
				int ingID = ingredients.getInt("storage_id");
				String updateSQL = "update Storage set amount = amount - ? where ID = ?";
				try (PreparedStatement stmt = connection.prepareStatement(updateSQL)) {
					stmt.setInt(1, amt);
					stmt.setInt(2, ingID);
					stmt.executeUpdate();
				} catch (SQLException e) {
					e.printStackTrace();
					return "{\"status\": \"error\"}";
				}
			}
		} catch (SQLException e) {
			System.out.println("SQL EXCEPTION: " + e.getMessage());
			return "{\"status\": \"error\"}";
		}

		return String.format("{\"status\": \"ok\",\"id\": %d}", newPalletID);
	}
}
