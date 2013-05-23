using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    public class TestTreeGenerator
    {
        public TestTreeGenerator(int level, int rootCellsCount)
        {
        }

        public TestTreeGenerator WithCellGenerator(Func<int, KeyValuePair<IdxKey, byte[]>> generator)
        {
            return this;
        }

        public TestTreeGenerator WithLeafSize(Func<int, int> size)
        {
            return this;
        }

        public TestTreeGenerator WithInteriorSize(Func<int, int, int> size)
        {
            return this;
        }

        private class RandomCellGeneratorInternal
        {
            Random random;
            int keyMinLength;
            int keyMaxLength;
            int valueMinLength;
            int valueMaxLength;

            public RandomCellGeneratorInternal(int keyMinLength, int keyMaxLength, int valueMinLength, int valueMaxLength)
            {
                this.random = new Random(84454);
                this.keyMinLength = keyMinLength; 
                this.keyMaxLength = keyMaxLength;
                this.valueMinLength = valueMinLength;
                this.valueMaxLength = valueMaxLength;
            }

            public KeyValuePair<IdxKey, byte[]> GetCell(int index)
            {
                var keyBase = IdxKey.FromInteger(index);
                var keyLength = random.Next(keyMinLength, keyMaxLength);
                byte[] keyBytes = new byte[keyLength];
                if (keyLength < 4)
                    Array.Copy(keyBase.ToBytes(), 4 - keyLength, keyBytes, 0, keyLength);
                else
                    Array.Copy(keyBase.ToBytes(), keyBytes, 4);

                var valueBytes = new byte[ValueLength()];
                random.NextBytes(valueBytes);

                return new KeyValuePair<IdxKey, byte[]>(IdxKey.FromBytes(keyBytes), valueBytes);
            }

            private int ValueLength()
            {
                if (valueMinLength == valueMaxLength)
                    return valueMinLength;
                var valueLengthBase = random.Next(0, 256);
                var valueLengthNormalized = (valueLengthBase * valueLengthBase * valueLengthBase) >> 16;
                var valueLength = valueMinLength + (valueMaxLength - valueMinLength) * valueLengthNormalized / 256;
                return valueLength;
            }
        }

        public static Func<int, KeyValuePair<IdxKey, byte[]>> RandomCellGenerator(int keyMinLength, int keyMaxLength, int valueMinLength, int valueMaxLength)
        {
            return new RandomCellGeneratorInternal(keyMinLength, keyMaxLength, valueMinLength, valueMaxLength).GetCell;
        }

        public IIdxContainer BuildContainer()
        {
            throw new NotImplementedException();
        }

        public KeyValuePair<IdxKey, byte[]>[] GetLeafCells(IdxKey min, IdxKey max)
        {
            throw new NotImplementedException();
        }
    }
}
