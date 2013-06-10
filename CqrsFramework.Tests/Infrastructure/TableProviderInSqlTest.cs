using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.Tests.Infrastructure
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    [TestClass]
    public class TableProviderInSqlTest
    {
        private DatabaseMock _db;
        private DataTable _dataTable;
        private List<TableProviderColumn> _columns;

        [TestInitialize]
        public void Initialize()
        {
            _columns = new List<TableProviderColumn>();
            _columns.Add(new TableProviderColumn(1, "name", typeof(string), true));
            _columns.Add(new TableProviderColumn(2, "version", typeof(int), false));
            _columns.Add(new TableProviderColumn(3, "guid", typeof(Guid), true));
            _columns.Add(new TableProviderColumn(4, "price", typeof(decimal), false));
            _columns.Add(new TableProviderColumn(5, "created", typeof(DateTime), false));
            _columns.Add(new TableProviderColumn(6, "data", typeof(byte[]), false));

            _dataTable = new DataTable("testtable");
            _dataTable.Columns.Add("id", typeof(int));
            foreach (var column in _columns)
                _dataTable.Columns.Add(column.Name, column.Type);

            InsertDataRow(1, "agg-1", 1, Guid.NewGuid(), 5.2m, new DateTime(2012, 10, 8), "Hello");
            InsertDataRow(2, "agg-1", 2, null, 1.2m, new DateTime(2012, 10, 9), "Hi");
            InsertDataRow(3, "agg-1", 3, Guid.NewGuid(), null, new DateTime(2012, 11, 8), "See ya");
            InsertDataRow(4, "agg-2", 1, Guid.NewGuid(), 2m, new DateTime(2013, 1, 1), "Thanks");

            _db = new DatabaseMock();
        }

        [TestCleanup]
        public void Cleanup()
        {
            _dataTable.Dispose();
        }

        private void InsertDataRow(int id, string name, int version, Guid? guid, decimal? price, DateTime? created, byte[] data)
        {
            var dataRow = _dataTable.NewRow();
            dataRow.SetField<int>(0, id);
            dataRow.SetField<string>(1, name);
            dataRow.SetField<int>(2, version);
            dataRow.SetField<Guid?>(3, guid);
            dataRow.SetField<decimal?>(4, price);
            dataRow.SetField<DateTime?>(5, created);
            dataRow.SetField<byte[]>(6, data);
            _dataTable.Rows.Add(dataRow);
        }

        private void InsertDataRow(int id, string name, int version, Guid? guid, decimal? price, DateTime? created, string stringData)
        {
            byte[] data = Encoding.ASCII.GetBytes(stringData);
            InsertDataRow(id, name, version, guid, price, created, data);
        }

        [TestMethod]
        public void HasColumns()
        {
            using (ITableProvider table = new SqlTableProvider(_db.CreateConnection, "testtable", _columns))
            {
                var columnList = table.GetColumns();
                CollectionAssert.AreEqual(_columns, columnList);
            }
        }

        [TestMethod]
        public void GetAllRows()
        {
            _db.SelectCommand(
                "SELECT id, name, version, guid, price, created, data FROM testtable",
                null, null, _dataTable);
            using (ITableProvider table = new SqlTableProvider(_db.CreateConnection, "testtable", _columns))
            {
                var rows = table.GetRows().ToList();
                Assert.AreEqual(4, rows.Count);
                Assert.IsTrue(rows.All(r => r.RowNumber > 0));
            }
        }

        [TestMethod]
        public void GettingRowsQueriesDatabase()
        {
            _db.SelectCommand(
                "SELECT id, name, version, guid, price, created, data FROM testtable WHERE name = @name AND version >= @version",
                (r, p) => r.Field<string>("name") == p.Get<string>("@name") && r.Field<int>("version") >= p.Get<int>("@version"),
                null, _dataTable);
            using (ITableProvider table = new SqlTableProvider(_db.CreateConnection, "testtable", _columns))
            {
                var filtered = table.GetRows().Where("name").Is("agg-1").And("version").IsAtLeast(0);
                var rows = filtered.ToList();
                Assert.AreEqual(3, rows.Count);
                Assert.IsTrue(rows.All(r => r.RowNumber > 0));
                Assert.IsTrue(rows.All(r => r.Get<string>("name") == "agg-1"));
                Assert.IsNull(rows.Single(r => r.RowNumber == 2)["guid"]);
            }
        }

        [TestMethod]
        public void InsertingNewRow()
        {
            _db.AggregateCommand(
                "SELECT MAX(id) FROM testtable",
                "MAX", "id", null, _dataTable);
                
            _db.ModifyCommand(
                "INSERT INTO testtable (id, name, version, guid, price, created, data) " +
                "VALUES (@id, @name, @version, @guid, @price, @created, @data)",
                (p, t) => InsertDataRow(
                    p.Get<int>("@id"), p.Get<string>("@name"), p.Get<int>("@version"), p.Get<Guid?>("@guid"),
                    p.Get<decimal?>("@price"), p.Get<DateTime?>("@created"), p.Get<byte[]>("@data")),
                _dataTable);
            using (var table = new SqlTableProvider(_db.CreateConnection, "testtable", _columns))
            {
                var row = table.NewRow();
                row[1] = "agg-2";
                row[2] = 3;
                row[3] = Guid.NewGuid();
                row[4] = 5.8m;
                row[5] = null;
                row[6] = Encoding.ASCII.GetBytes("Testing insert");
                table.Insert(row);
                Assert.IsTrue(row.RowNumber > 0);
                var dataRow = _dataTable.AsEnumerable().Single(r => r.Field<int>("id") == row.RowNumber);
                Assert.AreEqual("agg-2", dataRow.Field<string>("name"));
            }
        }

        [TestMethod]
        public void UpdatingExistingRow()
        {
            _db.SelectCommand(
                "SELECT id, name, version, guid, price, created, data FROM testtable WHERE id = @id",
                (r, p) => r.Field<int>("id") == p.Get<int>("@id"), null, _dataTable);
            _db.ModifyCommand(
                "UPDATE testtable SET name = @name, version = @version, guid = @guid, " +
                "price = @price, created = @created, data = @data WHERE id = @id",
                (p, t) =>
                {
                    var row = t.AsEnumerable().Single(r => r.Field<int>("id") == p.Get<int>("@id"));
                    row.SetField<string>("name", p.Get<string>("@name"));
                    row.SetField<int?>("version", p.Get<int?>("@version"));
                    row.SetField<Guid?>("guid", p.Get<Guid?>("@guid"));
                    row.SetField<decimal?>("price", p.Get<decimal?>("@price"));
                    row.SetField<DateTime?>("created", p.Get<DateTime?>("@created"));
                    row.SetField<byte[]>("data", p.Get<byte[]>("@data"));
                }, _dataTable);
            using (var table = new SqlTableProvider(_db.CreateConnection, "testtable", _columns))
            {
                var guid = Guid.NewGuid();
                var row = table.GetRows().Where("id").Is(2).Single();
                row[3] = guid;
                table.Update(row);
                var dataRow = _dataTable.AsEnumerable().Single(r => r.Field<int>("id") == 2);
                Assert.AreEqual("agg-1", dataRow.Field<string>("name"));
                Assert.AreEqual(guid, dataRow.Field<Guid>("guid"));
            }
        }

        [TestMethod]
        public void DeletingRow()
        {
            _db.SelectCommand(
                "SELECT id, name, version, guid, price, created, data FROM testtable WHERE id = @id",
                (r,p) => r.Field<int>("id") == p.Get<int>("@id"), null, _dataTable);
            _db.ModifyCommand(
                "DELETE FROM testtable WHERE id = @id",
                (p, t) => t.Rows.Remove(t.AsEnumerable().Single(r => r.Field<int>("id") == p.Get<int>("@id"))),
                _dataTable);
            using (var table = new SqlTableProvider(_db.CreateConnection, "testtable", _columns))
            {
                var row = table.GetRows().Where("id").Is(3).Single();
                table.Delete(row);
                Assert.IsFalse(_dataTable.AsEnumerable().Any(r => r.Field<int>("id") == 3));
                Assert.AreEqual(3, _dataTable.Rows.Count);
            }
        }
    }
}
