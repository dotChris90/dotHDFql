using System;
using Xunit;

namespace test.hdfqlLib
{
    public class HDFqlTester : IDisposable
    {
        protected string _hdfFileName;
        protected double[] _doubleVector;
        protected double[,] _doubleMatrix;
        protected int[,] _intMatrix;
        protected double[,,] _doubleTensor;
        protected void CreateGroups()
        {
            int errorCode = AS.HDFql.HDFql.Execute("CREATE GROUP group1");
            Assert.Equal(0,errorCode);
            errorCode = AS.HDFql.HDFql.Execute("CREATE GROUP group2");
            Assert.Equal(0,errorCode);    
        }
        protected void CreateDataSets()
        {
            int errorCode = AS.HDFql.HDFql.Execute("CREATE DATASET group1/set1 AS DOUBLE(1,100)");
            Assert.Equal(0,errorCode);
            errorCode = AS.HDFql.HDFql.Execute("CREATE DATASET group2/set1 AS INT(3,3)");
            Assert.Equal(0,errorCode);    
            errorCode = AS.HDFql.HDFql.Execute("CREATE DATASET group1/set2 AS DOUBLE(2,2)");
            Assert.Equal(0,errorCode);

            for (int idx = 0; idx < 15;idx++)
            {
                string cmd = "CREATE DATASET group2/set2" + idx.ToString() +  " AS DOUBLE(100,1000,1000)";
                errorCode = AS.HDFql.HDFql.Execute(cmd);
                Assert.Equal(0,errorCode);
            }

        }
        protected void InsertArrays()
        {
            _doubleVector =  new double[100];
            for (int idx = 0; idx < _doubleVector.Length;idx++)
            {
                _doubleVector[idx] = 2.0 * idx + 1; 
            }
            AS.HDFql.HDFql.VariableRegister(_doubleVector);
            AS.HDFql.HDFql.Execute("INSERT INTO group1/set1 VALUES FROM MEMORY 0");
            AS.HDFql.HDFql.VariableUnregister(_doubleVector);

            _doubleMatrix =  new double[2,2] {{1,2},{3,4}} ;

            AS.HDFql.HDFql.VariableRegister(_doubleMatrix);
            AS.HDFql.HDFql.Execute("INSERT INTO group1/set2 VALUES FROM MEMORY 0");
            AS.HDFql.HDFql.VariableUnregister(_doubleMatrix);

            _intMatrix =  new int[3,3] {{1,2,3},{4,5,6},{7,8,9}};

            AS.HDFql.HDFql.VariableRegister(_intMatrix);
            AS.HDFql.HDFql.Execute("INSERT INTO group2/set1 VALUES FROM MEMORY 0");
            AS.HDFql.HDFql.VariableUnregister(_intMatrix);

            var loalDoubleTensor = new double[100,1000,1000];

            AS.HDFql.HDFql.VariableRegister(loalDoubleTensor);
            for (int idx = 0; idx < 15;idx++)
            {
                string cmd = "INSERT INTO group2/set2" + idx.ToString() +  " VALUES FROM MEMORY 0";
                AS.HDFql.HDFql.Execute(cmd);
            }
            AS.HDFql.HDFql.VariableUnregister(loalDoubleTensor);
        }
        public HDFqlTester()
        {
            _hdfFileName = "myHDFFile.h5";
            AS.HDFql.HDFql.Execute("CREATE FILE " + _hdfFileName);
            AS.HDFql.HDFql.Execute("USE FILE " + _hdfFileName);
            this.CreateGroups();
            this.CreateDataSets();
            this.InsertArrays();
        }
        public void Dispose()
        {
            AS.HDFql.HDFql.Execute("CLOSE FILE " + _hdfFileName);
            AS.HDFql.HDFql.Execute("DROP FILE " + _hdfFileName);
        }
        [Fact]
        public void CheckGroupCreation()
        {
            AS.HDFql.HDFql.Execute("SHOW /");
            int resultCardinality = AS.HDFql.HDFql.CursorGetCount();
            Assert.Equal(2,resultCardinality);

            int errorCode = AS.HDFql.HDFql.CursorFirst();
            Assert.Equal(0,errorCode);

            string groupName = AS.HDFql.HDFql.CursorGetChar();
            Assert.Equal("group1",groupName);

            AS.HDFql.HDFql.CursorNext();

            groupName = AS.HDFql.HDFql.CursorGetChar();
            Assert.Equal("group2",groupName);
        }
        [Fact]
        public void CheckDataSetCreation()
        {
            AS.HDFql.HDFql.Execute("SHOW DATASET group1/");
            int resultCardinality = AS.HDFql.HDFql.CursorGetCount();
            Assert.Equal(2,resultCardinality);

            int errorCode = AS.HDFql.HDFql.CursorFirst();
            Assert.Equal(0,errorCode);

            string setName = AS.HDFql.HDFql.CursorGetChar();
            Assert.Equal("set1",setName);

            AS.HDFql.HDFql.CursorNext();

            setName = AS.HDFql.HDFql.CursorGetChar();
            Assert.Equal("set2",setName);

            AS.HDFql.HDFql.Execute("SHOW DATASET group2/");
            resultCardinality = AS.HDFql.HDFql.CursorGetCount();
            Assert.Equal(2,resultCardinality);

            errorCode = AS.HDFql.HDFql.CursorFirst();
            Assert.Equal(0,errorCode);

            setName = AS.HDFql.HDFql.CursorGetChar();
            Assert.Equal("set1",setName);
        }
        [Fact]
        public void CheckInsert()
        {
            double[] doubleVectorStore = new double[100];
            double[,] doubleMatrixStore = new double[2,2];
            int[,] intMatrixStore = new int[3,3];

            AS.HDFql.HDFql.VariableRegister(doubleVectorStore);
            AS.HDFql.HDFql.Execute("SELECT FROM group1/set1 INTO MEMORY " + AS.HDFql.HDFql.VariableGetNumber(doubleVectorStore));
            AS.HDFql.HDFql.VariableUnregister(doubleVectorStore);

            Assert.Equal(_doubleVector,doubleVectorStore);

            AS.HDFql.HDFql.VariableRegister(doubleMatrixStore);
            AS.HDFql.HDFql.Execute("SELECT FROM group1/set2 INTO MEMORY " + AS.HDFql.HDFql.VariableGetNumber(doubleMatrixStore));
            AS.HDFql.HDFql.VariableUnregister(doubleMatrixStore);

            Assert.Equal(_doubleMatrix,doubleMatrixStore);

            AS.HDFql.HDFql.VariableRegister(intMatrixStore);
            AS.HDFql.HDFql.Execute("SELECT FROM group2/set1 INTO MEMORY " + AS.HDFql.HDFql.VariableGetNumber(intMatrixStore));
            AS.HDFql.HDFql.VariableUnregister(intMatrixStore);

            Assert.Equal(_intMatrix,intMatrixStore);
        }

    }
}
