using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;
using CqrsFramework.EventStore;
using Moq;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreFilesIndexFileTest
    {
        private Mock<IIdxTree> _streams;
        private Mock<IIdxTree> _headers;

        private IdxKey _appendPositionKey = IdxKey.FromBytes(new byte[1] { 0 });
        private IdxKey _unpublishedPositionKey = IdxKey.FromBytes(new byte[1] { 1 });
        private byte[] _appendPositionBytes;
        private byte[] _unpublishedPositionBytes;

        [TestInitialize]
        public void Initialize()
        {
            _headers = new Mock<IIdxTree>(MockBehavior.Strict);
            _streams = new Mock<IIdxTree>(MockBehavior.Strict);

            _headers
                .Setup(h => h.Select(It.IsAny<IdxKey>(), It.IsAny<IdxKey>()))
                .Returns<IdxKey, IdxKey>(SelectFromHeaders);
            _headers
                .Setup(h => h.Insert(_appendPositionKey, It.IsAny<byte[]>()))
                .Callback<IdxKey, byte[]>((k, v) => SetHeaderValue(1, v, false));
            _headers
                .Setup(h => h.Insert(_unpublishedPositionKey, It.IsAny<byte[]>()))
                .Callback<IdxKey, byte[]>((k, v) => SetHeaderValue(2, v, false));
            _headers
                .Setup(h => h.Update(_appendPositionKey, It.IsAny<byte[]>()))
                .Callback<IdxKey, byte[]>((k, v) => SetHeaderValue(1, v, true));
            _headers
                .Setup(h => h.Update(_unpublishedPositionKey, It.IsAny<byte[]>()))
                .Callback<IdxKey, byte[]>((k, v) => SetHeaderValue(2, v, true));
        }

        private IEnumerable<KeyValuePair<IdxKey, byte[]>> SelectFromHeaders(IdxKey min, IdxKey max)
        {
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            if (_appendPositionBytes != null)
                result.Add(new KeyValuePair<IdxKey, byte[]>(_appendPositionKey, _appendPositionBytes));
            if (_unpublishedPositionBytes != null)
                result.Add(new KeyValuePair<IdxKey, byte[]>(_unpublishedPositionKey, _unpublishedPositionBytes));
            return result.Where(i => min <= i.Key && i.Key <= max).ToList();
        }

        private void SetHeaderValue(int key, byte[] value, bool isUpdate)
        {
            var keyName = new string[] { "", "append", "unpublished" };
            var origValue = new byte[][] { null, _appendPositionBytes, _unpublishedPositionBytes };
            Assert.IsNotNull(value, "Set to null {0} bytes", keyName[key]);
            if (isUpdate)
                Assert.IsNotNull(origValue[key], "Null {0} bytes", keyName[key]);
            else
                Assert.IsNull(origValue[key], "Null {0} bytes", keyName[key]);
            if (key == 1)
                _appendPositionBytes = value;
            else if (key == 2)
                _unpublishedPositionBytes = value;
        }

        private byte[] LongToBytes(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        private IdxKey StreamKey(string streamId, bool snapshot, int version)
        {
            var stringBytes = Encoding.ASCII.GetBytes(streamId);
            var lengthBytes = BitConverter.GetBytes((short)stringBytes.Length);
            var typeBytes = new byte[1] { snapshot ? (byte)0 : (byte)1 };
            var versionBytes = BitConverter.GetBytes(version);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lengthBytes);
                Array.Reverse(versionBytes);
            }
            var bytes = lengthBytes.Concat(stringBytes).Concat(typeBytes).Concat(versionBytes).ToArray();
            return IdxKey.FromBytes(bytes);
        }

        [TestMethod]
        public void GetPositionsInEmpty()
        {
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            Assert.AreEqual(0, index.UnpublishedPosition);
            Assert.AreEqual(0, index.AppendPosition);
        }

        [TestMethod]
        public void SetPositionsInEmpty()
        {
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            index.AppendPosition = 472235447;
            index.UnpublishedPosition = 2512745;
            index.Flush();
            AssertExtension.AreEqual(LongToBytes(472235447), _appendPositionBytes, "Append position");
            AssertExtension.AreEqual(LongToBytes(2512745), _unpublishedPositionBytes, "Unpublished position");
        }

        [TestMethod]
        public void GetPositionsInUsed()
        {
            _appendPositionBytes = LongToBytes(544);
            _unpublishedPositionBytes = LongToBytes(333);
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            Assert.AreEqual(333, index.UnpublishedPosition);
            Assert.AreEqual(544, index.AppendPosition);
        }

        [TestMethod]
        public void SetPositionsInUsed()
        {
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            index.AppendPosition = 472235447;
            index.UnpublishedPosition = 2512745;
            index.Flush();
            AssertExtension.AreEqual(LongToBytes(472235447), _appendPositionBytes, "Append position");
            AssertExtension.AreEqual(LongToBytes(2512745), _unpublishedPositionBytes, "Unpublished position");
        }

        [TestMethod]
        public void AddRecord()
        {
            _streams.Setup(s => s.Insert(It.IsAny<IdxKey>(), It.IsAny<byte[]>())).Callback<IdxKey, byte[]>((k, v) =>
                {
                    Assert.AreEqual(StreamKey("Aggregate:111", false, 14), k, "Key");
                    AssertExtension.AreEqual(LongToBytes(85447), v, "Value");
                }).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            index.AddEvent("Aggregate:111", 14, 85447);
            _streams.Verify();
        }

        [TestMethod]
        public void AddSnapshot()
        {
            _streams.Setup(s => s.Insert(It.IsAny<IdxKey>(), It.IsAny<byte[]>())).Callback<IdxKey, byte[]>((k, v) =>
            {
                Assert.AreEqual(StreamKey("Snapshot:588", true, 8), k, "Key");
                AssertExtension.AreEqual(LongToBytes(96674), v, "Value");
            }).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            index.AddSnapshot("Snapshot:588", 8, 96674);
            _streams.Verify();
        }

        [TestMethod]
        public void FindAllEvents()
        {
            var minKey = StreamKey("Agg:5957", false, 0);
            var maxKey = StreamKey("Agg:5957", false, int.MaxValue);
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            var expectedPositions = new List<long>();
            for (int i = 1; i <= 17; i++)
            {
                var key = StreamKey("Agg:5957", false, i);
                var position = 2847 + 68 * i;
                var value = LongToBytes(position);
                result.Add(new KeyValuePair<IdxKey, byte[]>(key, value));
                expectedPositions.Add(position);
            }
            _streams.Setup(s => s.Select(minKey, maxKey)).Returns(result).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            IEnumerable<long> eventPositions = index.FindEvents("Agg:5957", 0);
            _streams.Verify();
            AssertExtension.AreEqual(expectedPositions.ToArray(), eventPositions.ToArray());
        }

        [TestMethod]
        public void FindSnapshot()
        {
            var minKey = StreamKey("Agg:5957", true, 0);
            var maxKey = StreamKey("Agg:5957", true, int.MaxValue);
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            result.Add(new KeyValuePair<IdxKey, byte[]>(StreamKey("Agg:5957", true, 4), LongToBytes(29957)));
            result.Add(new KeyValuePair<IdxKey, byte[]>(StreamKey("Agg:5957", true, 9), LongToBytes(52117)));
            result.Add(new KeyValuePair<IdxKey, byte[]>(StreamKey("Agg:5957", true, 15), LongToBytes(68521)));
            _streams.Setup(s => s.Select(minKey, maxKey)).Returns(result).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            long snapshotPosition = index.FindSnapshot("Agg:5957");
            _streams.Verify();
            Assert.AreEqual(68521, snapshotPosition);
        }

        [TestMethod]
        public void FindEventsFromVersion()
        {
            var minKey = StreamKey("Agg:5957", false, 8);
            var maxKey = StreamKey("Agg:5957", false, int.MaxValue);
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            var expectedPositions = new List<long>();
            for (int i = 8; i <= 17; i++)
            {
                var key = StreamKey("Agg:5957", false, i);
                var position = 2847 + 68 * i;
                var value = LongToBytes(position);
                result.Add(new KeyValuePair<IdxKey, byte[]>(key, value));
                expectedPositions.Add(position);
            }
            _streams.Setup(s => s.Select(minKey, maxKey)).Returns(result).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            IEnumerable<long> eventPositions = index.FindEvents("Agg:5957", 8);
            _streams.Verify();
            AssertExtension.AreEqual(expectedPositions.ToArray(), eventPositions.ToArray());
        }

        [TestMethod]
        public void CheckThatStreamDoesNotExist()
        {
            var minKey = StreamKey("Aggregate:5957", false, 0);
            var maxKey = StreamKey("Aggregate:5957", false, 1);
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            _streams.Setup(s => s.Select(minKey, maxKey)).Returns(result).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            var streamExists = index.StreamExists("Aggregate:5957");
            _streams.Verify();
            Assert.IsFalse(streamExists);
        }

        [TestMethod]
        public void CheckThatStreamExists()
        {
            var minKey = StreamKey("Aggregate:5957", false, 0);
            var maxKey = StreamKey("Aggregate:5957", false, 1);
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            result.Add(new KeyValuePair<IdxKey,byte[]>(StreamKey("Aggregate:5957", false, 1), LongToBytes(5844)));
            _streams.Setup(s => s.Select(minKey, maxKey)).Returns(result).Verifiable();
            var index = new FileEventStoreIndexCore(_headers.Object, _streams.Object);
            var streamExists = index.StreamExists("Aggregate:5957");
            _streams.Verify();
            Assert.IsTrue(streamExists);
        }
    }
}
