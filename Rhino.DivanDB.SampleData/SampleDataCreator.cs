using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.SampleData.NwindDataSetTableAdapters;
using Newtonsoft.Json;

namespace Rhino.DivanDB.SampleData
{
    public class SampleDataCreator
    {
        public static void InsertNorthwindData(DocumentDatabase db)
        {
            NwindDataSet ds = new NwindDataSet();
            DataTable dt;
            List<JObject> jsonObjects;

            //categories
            var catTable = new CategoriesTableAdapter();
            catTable.Fill(ds.Categories);
            dt = catTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);
            foreach (JObject cat in jsonObjects)
            {
                cat["ObjectType"] = JToken.FromObject("Category");
                db.Put(cat);
            }
            var categoryIDs_Keys = jsonObjects.Select(cat => new
            {
                CategoryID = cat["CategoryID"].Value<int>(),
                Key = cat["_id"].Value<string>()
            }).ToList();

            //customers
            var custTable = new CustomersTableAdapter();
            custTable.Fill(ds.Customers);
            dt = custTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);
            foreach (JObject json in jsonObjects)
            {
                json["ObjectType"] = JToken.FromObject("Customer");
                db.Put(json);
            }
            var customerIDs_Keys = jsonObjects.Select(cust => new
            {
                CustomerID = cust["CustomerID"].Value<string>(),
                Key = cust["_id"].Value<string>()
            }).ToList();

            //employees
            var empTable = new EmployeesTableAdapter();
            empTable.Fill(ds.Employees);
            dt = empTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);
            foreach (JObject json in jsonObjects)
            {
                json["ObjectType"] = JToken.FromObject("Employee");
                db.Put(json);
            }
            //update ReportsTo with id
            var employeeIDs_Keys = jsonObjects.Select(emp => new
            {
                EmployeeID = emp["EmployeeID"].Value<int>(),
                Key = emp["_id"].Value<string>()
            }).ToList();

            foreach (JObject json in jsonObjects)
            {
                if (json["ReportsTo"].Value<int?>().HasValue)
                {
                    JToken token = JToken.FromObject(employeeIDs_Keys.Single(idk => idk.EmployeeID == json["ReportsTo"].Value<int>()).Key);
                    json["ReportsTo"] = token;
                    db.Put(json);
                }
            }

            //suppliers
            var suppTable = new SuppliersTableAdapter();
            suppTable.Fill(ds.Suppliers);
            dt = suppTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);
            foreach (JObject json in jsonObjects)
            {
                json["ObjectType"] = JToken.FromObject("Supplier");
                db.Put(json);
            }
            var supplierIDs_Keys = jsonObjects.Select(supplier => new
            {
                SupplierID = supplier["SupplierID"].Value<int>(),
                Key = supplier["_id"].Value<string>()
            }).ToList();

            //shippers
            var shipperTable = new ShippersTableAdapter();
            shipperTable.Fill(ds.Shippers);
            dt = shipperTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);
            foreach (JObject json in jsonObjects)
            {
                json["ObjectType"] = JToken.FromObject("Shipper");
                db.Put(json);
            }
            var shipperIDs_Keys = jsonObjects.Select(shipper => new
            {
                ShipperID = shipper["ShipperID"].Value<int>(),
                Key = shipper["_id"].Value<string>()
            }).ToList();

            //products
            var productTable = new ProductsTableAdapter();
            productTable.Fill(ds.Products);
            dt = productTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);

            foreach (JObject json in jsonObjects)
            {
                if (json["SupplierID"].Value<int?>().HasValue)
                    json["SupplierID"] = JToken.FromObject(supplierIDs_Keys.Single(idk => idk.SupplierID == json["SupplierID"].Value<int>()).Key);
                if (json["CategoryID"].Value<int?>().HasValue)
                    json["CategoryID"] = JToken.FromObject(categoryIDs_Keys.Single(idk => idk.CategoryID == json["CategoryID"].Value<int>()).Key);
                json["ObjectType"] = JToken.FromObject("Product");
                db.Put(json);
            }
            var productIDs_Keys = jsonObjects.Select(product => new
            {
                ProductID = product["ProductID"].Value<int>(),
                Key = product["_id"].Value<string>()
            }).ToList();

            //orders
            var orderTable = new OrdersTableAdapter();
            orderTable.Fill(ds.Orders);
            dt = orderTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);

            foreach (JObject json in jsonObjects)
            {
                if (!string.IsNullOrEmpty(json["CustomerID"].Value<string>()))
                    json["CustomerID"] = JToken.FromObject(customerIDs_Keys.Single(idk => idk.CustomerID == json["CustomerID"].Value<string>()).Key);
                if (json["EmployeeID"].Value<int?>().HasValue)
                    json["EmployeeID"] = JToken.FromObject(employeeIDs_Keys.Single(idk => idk.EmployeeID == json["EmployeeID"].Value<int>()).Key);
                if (json["ShipVia"].Value<int?>().HasValue)
                    json["ShipVia"] = JToken.FromObject(shipperIDs_Keys.Single(idk => idk.ShipperID == json["ShipVia"].Value<int>()).Key);
                json["ObjectType"] = JToken.FromObject("Order");
                db.Put(json);
            }
            var orderIDs_Keys = jsonObjects.Select(order => new
            {
                OrderID = order["OrderID"].Value<int>(),
                Key = order["_id"].Value<string>()
            }).ToList();

            //order details
            var orderDetailsTable = new Order_DetailsTableAdapter();
            orderDetailsTable.Fill(ds.Order_Details);
            dt = orderDetailsTable.GetData();
            jsonObjects = GetJsonObjectsFromDataTable(dt);

            foreach (JObject json in jsonObjects)
            {
                if (json["OrderID"].Value<int?>().HasValue)
                    json["OrderID"] = JToken.FromObject(orderIDs_Keys.Single(idk => idk.OrderID == json["OrderID"].Value<int>()).Key);
                if (json["ProductID"].Value<int?>().HasValue)
                    json["ProductID"] = JToken.FromObject(productIDs_Keys.Single(idk => idk.ProductID == json["ProductID"].Value<int>()).Key);
                json["ObjectType"] = JToken.FromObject("OrderDetail");
                db.Put(json);
            }

            CreateIndexes(db);
        }

        private static void CreateIndexes(DocumentDatabase db)
        {
            db.PutIndex("categoriesByName",
                @"
                from doc in docs         
                where doc.ObjectType == ""Category""       
                select new { doc.CategoryName };
            ");

            db.PutIndex("customersByName",
            @"
                from doc in docs         
                where doc.ObjectType == ""Customer""       
                select new { doc.ContactName };
            ");
            
            db.PutIndex("employeesByLastName",
                @"
                from doc in docs         
                where doc.ObjectType == ""Employee""       
                select new { doc.LastName };
            ");

            db.PutIndex("ordersByCountry",
                @"
                from doc in docs         
                where doc.ObjectType == ""Order""       
                select new { doc.ShipCountry };
            ");
            db.PutIndex("productsByName",
                @"
                from doc in docs         
                where doc.ObjectType == ""Product""       
                select new { doc.ProductName };
            ");
            db.PutIndex("shippersByName",
                @"
                from doc in docs         
                where doc.ObjectType == ""Shipper""       
                select new { doc.CompanyName };
            ");
            db.PutIndex("suppliersByCompanyName",
                @"
                from doc in docs         
                where doc.ObjectType == ""Supplier""       
                select new { doc.CompanyName };
            ");
        }

        private static List<JObject> GetJsonObjectsFromDataTable(DataTable dt)
        {
            List<JObject> jsonObjects = new List<JObject>();
            foreach (DataRow row in dt.Rows)
            {
                Dictionary<string, object> values = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    if (row[col].GetType() != typeof(byte[]))
                    {
                        if (row[col] == DBNull.Value)
                            values.Add(col.ColumnName, null);
                        else
                            values.Add(col.ColumnName, row[col]);
                    }
                }
                JObject jobj = JObject.Parse(JsonConvert.SerializeObject(values));
                jsonObjects.Add(jobj);
            }
            return jsonObjects;
        }
    }
}
