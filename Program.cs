using System;
using System.Xml.Linq;
using System.Globalization;
using MySqlConnector;

class Program
{
    static string connectionString = "Server=sql7.freesqldatabase.com;Database=sql7732467;Uid=sql7732467;Pwd=PWMvUyhhPg;";

    static void Main(string[] args)
    {
        try
        {
            XDocument doc = XDocument.Load("data.xml");
            foreach (var orderElement in doc.Descendants("order"))
            {
                int orderId = InsertOrder(orderElement);
                InsertOrderItems(orderId, orderElement);
                InsertOrUpdateUser(orderId, orderElement);
            }
            Console.WriteLine("Data loaded successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static int InsertOrder(XElement orderElement)
    {
        string query = @"INSERT INTO ORDERS (order_date, total_amount, status) 
                         VALUES (@OrderDate, @TotalAmount, @Status);
                         SELECT LAST_INSERT_ID();";

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@OrderDate", DateTime.ParseExact(orderElement.Element("reg_date").Value, "yyyy.MM.dd", CultureInfo.InvariantCulture));
                command.Parameters.AddWithValue("@TotalAmount", decimal.Parse(orderElement.Element("sum").Value, CultureInfo.InvariantCulture));
                command.Parameters.AddWithValue("@Status", "Completed");
                return Convert.ToInt32(command.ExecuteScalar());
            }
        }
    }

    static void InsertOrderItems(int orderId, XElement orderElement)
    {
        string query = @"INSERT INTO ORDER_ITEMS (order_id, product_id, quantity, price) 
                         VALUES (@OrderId, @ProductId, @Quantity, @Price)";

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            foreach (var productElement in orderElement.Elements("product"))
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    int productId = GetOrCreateProduct(productElement.Element("name").Value);
                    command.Parameters.AddWithValue("@OrderId", orderId);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Quantity", int.Parse(productElement.Element("quantity").Value));
                    command.Parameters.AddWithValue("@Price", decimal.Parse(productElement.Element("price").Value, CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }

    static int GetOrCreateProduct(string productName)
    {
        string query = @"INSERT INTO PRODUCTS (name, category_id) 
                         SELECT @Name, 1 FROM DUAL 
                         WHERE NOT EXISTS (SELECT 1 FROM PRODUCTS WHERE name = @Name);
                         SELECT product_id FROM PRODUCTS WHERE name = @Name;";

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@Name", productName);
                return Convert.ToInt32(command.ExecuteScalar());
            }
        }
    }

    static void InsertOrUpdateUser(int orderId, XElement orderElement)
    {
        string query = @"INSERT INTO USERS (full_name, email) 
                         VALUES (@FullName, @Email)
                         ON DUPLICATE KEY UPDATE full_name = @FullName;
                         UPDATE ORDERS SET user_id = (SELECT user_id FROM USERS WHERE email = @Email) 
                         WHERE order_id = @OrderId";

        using (MySqlConnection connection = new MySqlConnection(connectionString))
        {
            connection.Open();
            using (MySqlCommand command = new MySqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@FullName", orderElement.Element("user").Element("fio").Value);
                command.Parameters.AddWithValue("@Email", orderElement.Element("user").Element("email").Value);
                command.Parameters.AddWithValue("@OrderId", orderId);
                command.ExecuteNonQuery();
            }
        }
    }
}