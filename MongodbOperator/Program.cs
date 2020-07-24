using System;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq;
using System.Reflection;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using System.Collections.Immutable;
using System.Globalization;
using ExcelDataReader;
using System.Data;


namespace MongodbOperator
{
    class Program
    {
        static void Main(string[] args)
        {
            //CopyCollection("news", "1_1000", "Combined");
            ConvertFirmList();
        }

        public struct QueryParam
        {
            public DateTime sDt;
            public DateTime eDt;
            public string companyName;
        }


        static void ConvertFirmList()
        {
            string filePath = $"{AppContext.BaseDirectory}\\firms.xls";
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
            var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
            var reader = ExcelReaderFactory.CreateReader(stream);
            var result = reader.AsDataSet();
            int totalRows = result.Tables[0].Rows.Count;

            List<QueryParam> queryParams = new List<QueryParam>();
            for (int rowIndex = 0; rowIndex < totalRows; rowIndex++)
            {
                queryParams.Add(new QueryParam()
                {
                    sDt = (DateTime)result.Tables[0].Rows[rowIndex][0],
                    eDt = ((DateTime)result.Tables[0].Rows[rowIndex][0]).AddMonths(6),
                    companyName = (string)result.Tables[0].Rows[rowIndex][1]
                });
            }
            queryParams = queryParams.OrderBy(n => n.companyName).ThenBy(n => n.sDt).ToList();

            string queryParamsStr = JsonConvert.SerializeObject(queryParams, Formatting.Indented);
            string fileName = $"{AppContext.BaseDirectory}\\queryParams.json";
            StreamWriter sw = new StreamWriter(fileName, false);
            sw.Write(queryParamsStr);
            sw.Close();
        }

        static void VerifyCompanyName()
        {
            StreamReader sr = new StreamReader($"{Directory.GetCurrentDirectory()}\\firm_list.csv");

            string line = sr.ReadLine();
            List<QueryParam> queryParams = new List<QueryParam>();

            while(sr.Peek() > 0)
            {
                line =  sr.ReadLine();
                string[] fields = line.Split(',');

                string date = fields[0];
                Match result = Regex.Match(date, @"^(\d+)/(\d+)/(\d+)");

                if (result.Groups.Count != 4)
                    continue;

                int month = int.Parse(result.Groups[1].ToString());
                int day = int.Parse(result.Groups[2].ToString());
                int year = int.Parse(result.Groups[3].ToString());

                if (year > 30)
                    year += 1900;
                else
                    year += 2000;

                DateTime eDt = new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc);
                DateTime sDt = eDt.AddYears(-1);

                queryParams.Add(new QueryParam()
                { 
                    companyName = fields[1],
                    sDt = sDt,
                    eDt = eDt,
                
                });
            }

            //Check company names
            MongoClientSettings mcs = new MongoClientSettings()
            {
                //Server = new MongoServerAddress("141.20.100.49", 27017)
                Server = new MongoServerAddress("127.0.0.1", 27017)
            };
            var client = new MongoClient(mcs);
            var database = client.GetDatabase("factiva");
            var collection = database.GetCollection<BsonDocument>("News");

            BsonDocument distinctCompanyNamesInDbFilter = new BsonDocument
            {
                {
                    "$group",
                    new BsonDocument
                    {
                        { "_id","$COMPANY"  }
                    }
                }
            };

            var pipline = new[] { distinctCompanyNamesInDbFilter };

            List<BsonDocument> companyNamesInDbBson = collection.Aggregate<BsonDocument>(pipline).ToList();
            List<string> companyNamesInDb = companyNamesInDbBson.Select(c => c["_id"].ToString()).ToList();

            List<QueryParam> filteredQueryParams = queryParams.Where(q => companyNamesInDb.Contains(q.companyName)).ToList();

            string serialized = JsonConvert.SerializeObject(filteredQueryParams, Formatting.Indented);
            StreamWriter sw = new StreamWriter($"{Directory.GetCurrentDirectory()}\\QueryParams.json");
            sw.Write(serialized);
            sw.Close();
        }


        static void CopyCollection(string databaseName, string sourceCollectionName, string targetCollectionName)
        {
            MongoClientSettings mcs = new MongoClientSettings()
            {
                //Server = new MongoServerAddress("141.20.100.49", 27017)
                Server = new MongoServerAddress("127.0.0.1", 27017)
            };
            var client = new MongoClient(mcs);
            var database = client.GetDatabase(databaseName);
            var sourceCollection = database.GetCollection<BsonDocument>(sourceCollectionName);
            var targetCollection = database.GetCollection<BsonDocument>(targetCollectionName);

            var noFilter = new BsonDocument();
            var cursor = sourceCollection.Find(noFilter).ToCursor();

            List<BsonDocument> buffer = new List<BsonDocument>();
            int bufCounter = 0;
            int totalCounts = 0;
            int missingCounts = 0;

            string[] fieldNames = new string[]
            {
                 "time", "headline", "body", "CompanyName"
            };

            while (cursor.MoveNext())
            {
                foreach (BsonDocument doc in cursor.Current)
                {
                    #region Conversion
                    BsonDocument bd = new BsonDocument();
                    bool fieldComplete = true;
                    foreach (string fieldName in fieldNames)
                    {
                        if (!doc.Contains(fieldName))
                        {
                            fieldComplete = false;
                            break;
                        }
                        object result = null;
                        string fieldContent = doc[fieldName].ToString();
                        switch (fieldName)
                        {
                            case "time":
                                if (DateTime.TryParse(fieldContent, CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal, out DateTime dtParseResult))
                                    result = dtParseResult;
                                else
                                    fieldComplete = false;
                                break;

                            case "CompanyName":
                                result = fieldContent.Trim();
                                break;

                            default:
                                result = fieldContent.Replace('\n', ' ').Trim();
                                break;
                        }
                        bd.Add(fieldName, BsonValue.Create(result));
                    }
                    
                    if (!fieldComplete)
                    {
                        missingCounts++;
                        continue;
                    }
                    
                    #endregion

                    buffer.Add(bd);
                    bufCounter++;
                    totalCounts++;

                    if(bufCounter > 100)
                    {
                        targetCollection.InsertMany(buffer);
                        Console.WriteLine($"has written {totalCounts}");
                        buffer.Clear();
                        bufCounter = 0;
                    }
                }
            }
            targetCollection.InsertMany(buffer);
            Console.WriteLine($"has written {totalCounts}. missing counts {missingCounts}");
        }

        static void CreateDateTimeField()
        {
            //var client = new MongoClient("mongodb+srv://<username>:<password>@<cluster-address>/test?w=majority");
            MongoClientSettings mcs = new MongoClientSettings()
            {
                //Server = new MongoServerAddress("141.20.100.49", 27017)
                Server = new MongoServerAddress("127.0.0.1", 27017)
            };

            var client = new MongoClient(mcs);
            var database = client.GetDatabase("factiva");
            var collection = database.GetCollection<BsonDocument>("statis_data3");

            var noFilter = new BsonDocument();
            var cursor = collection.Find(noFilter).ToCursor();

            while (cursor.MoveNext())
            {
                foreach (BsonDocument doc in cursor.Current)
                {
                    // do something with the documents

                    ObjectId id = doc.GetValue("_id").AsObjectId;
                    if (doc.Contains("DateTime"))
                    {
                        DateTime existedDt = doc["DateTime"].ToUniversalTime();
                        Console.WriteLine($"{existedDt.ToString()} {id.ToString()} existed. Skip");
                        continue;
                    }

                    string date = doc.GetValue("DATE").AsString;
                    Match result = Regex.Match(date, @"^(\d+)/(\d+)/(\d+)");

                    if (result.Groups.Count != 4)
                        continue;

                    int month = int.Parse(result.Groups[1].ToString());
                    int day = int.Parse(result.Groups[2].ToString());
                    int year = int.Parse(result.Groups[3].ToString());

                    if (year > 30)
                        year += 1900;
                    else
                        year += 2000;

                    DateTime dt = new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc);
                    BsonValue bv = BsonValue.Create(dt);
                    var update = new BsonDocument("$set", new BsonDocument("DateTime", bv));

                    var filter = new BsonDocument("_id", id);
                    var updateResult = collection.UpdateOne(filter, update);

                    Console.WriteLine($"{dt.ToString()} {id.ToString()}");

                }
            }
        }

    }
}
