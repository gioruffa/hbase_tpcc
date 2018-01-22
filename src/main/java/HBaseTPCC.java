import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class HBaseTPCC {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    String warehouseTableName = "warehouse";
    String warehouseColumnFamilyName = "warehousecf";

    String districtTableName = "district";
    String districtColumnFamilyName = "districtcf";

    String itemTableName = "item";
    String itemColumnFamilyName = "itemcf";

    String new_orderTableName = "new_order";
    String new_orderColumnFamilyName = "new_ordercf";

    String ordersTableName = "orders";
    String ordersColumnFamilyName = "orderscf";

    String historyTableName = "history";
    String historyColumnFamilyName = "historycf";

    String customerTableName = "customer";
    String customerColumnFamilyName = "customercf";

    String stockTableName = "stock";
    String stockColumnFamilyName = "stockcf";

    String order_lineTableName = "order_line";
    String order_lineColumnFamilyName = "order_linecf";
    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseTPCC(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    public void createTPCCTables() throws IOException {
        /**
         * Wharehouse
         */
        createTableWrapper(warehouseTableName ,new String[] {warehouseColumnFamilyName}, new Integer[]{1});
        createTableWrapper(districtTableName,new String[] {districtColumnFamilyName}, new Integer[]{1});
        createTableWrapper(itemTableName,new String[] {itemColumnFamilyName}, new Integer[]{1});
        createTableWrapper(new_orderTableName,new String[] {new_orderColumnFamilyName}, new Integer[]{1});
        createTableWrapper(ordersTableName,new String[] {ordersColumnFamilyName}, new Integer[]{1});
        createTableWrapper(historyTableName,new String[] {historyColumnFamilyName}, new Integer[]{1});
        createTableWrapper(customerTableName,new String[] {customerColumnFamilyName}, new Integer[]{4});
        createTableWrapper(stockTableName,new String[] {stockColumnFamilyName}, new Integer[]{1});
        createTableWrapper(order_lineTableName,new String[] {order_lineColumnFamilyName}, new Integer[]{1});
        System.exit(0);
    }

    public void loadTables(String folder)throws IOException{

        HConnection connection = HConnectionManager.createConnection(config);


        loadTableWrapper(
                connection,
                warehouseTableName,
                warehouseColumnFamilyName,
                folder,
                "warehouse.csv",
                new String[] {
                        "W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_TAX", "W_YTD"
                },
                new int [] {0}
        );

        loadTableWrapper(
                connection,
                districtTableName,
                districtColumnFamilyName,
                folder,
                "district.csv",
                new String[] {
                        "D_ID", "D_W_ID", "D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID"
                },
                new int [] {1,0}
        );

        loadTableWrapper(
                connection,
                itemTableName,
                itemColumnFamilyName,
                folder,
                "item.csv",
                new String[] {
                        "I_ID", "I_IM_ID", "I_NAME", "I_PRICE", "I_DATA"
                },
                new int [] {0}
        );

        loadTableWrapper(
                connection,
                new_orderTableName,
                new_orderColumnFamilyName,
                folder,
                "new_order.csv",
                new String[] {
                        "NO_O_ID", "NO_D_ID", "NO_W_ID"
                },
                new int [] {2,1,0}
        );

        loadTableWrapper(
                connection,
                ordersTableName,
                ordersColumnFamilyName,
                folder,
                "orders.csv",
                new String[] {
                        "O_ID", "O_D_ID", "O_W_ID", "O_C_ID", "O_ENTRY_D", "O_CARRIER_ID", "O_OL_CNT", "O_ALL_LOCAL"
                },
                new int [] {2,1,0}
        );

        loadTableWrapper(
                connection,
                historyTableName,
                historyColumnFamilyName,
                folder,
                "history.csv",
                new String[]{
                        "H_C_ID", "H_C_D_ID", "H_C_W_ID", "H_D_ID", "H_W_ID", "H_DATE", "H_AMOUNT", "H_DATA"
                },
                new int [] {4,3,5}
        );

        loadTableWrapper(
                connection,
                customerTableName,
                customerColumnFamilyName,
                folder,
                "customer.csv",
                new String[] {
                        "C_ID", "C_D_ID", "C_W_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1",
                        "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDITLIM",
                        "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"
                },
                new int [] {2,1,0}
        );

        loadTableWrapper(
                connection,
                stockTableName,
                stockColumnFamilyName,
                folder,
                "stock.csv",
                new String[] {
                        "S_I_ID", "S_W_ID", "S_QUANTITY", "S_DIST_01", "S_DIST_02", "S_DIST_03", "S_DIST_04",
                        "S_DIST_05", "S_DIST_06", "S_DIST_07", "S_DIST_08", "S_DIST_09", "S_DIST_10", "S_YTD",
                        "S_ORDER_CNT", "S_REMOTE_CNT", "S_DATA"
                },
                new int [] {1,0}
        );

        loadTableWrapper(
                connection,
                order_lineTableName,
                order_lineColumnFamilyName,
                folder,
                "order_line.csv",
                new String[] {
                        "OL_O_ID", "O_D_ID", "OL_W_ID", "OL_NUMBER", "OL_I_ID", "OL_SUPPLY_W_ID",
                        "OL_DELIVERY_D", "OL_QUANTITY", "OL_AMOUNT", "OL_DIST_INFO"
                },
                new int [] {2,1,0,3}
        );




        //TO IMPLEMENT
        System.out.println(String.format("The folder is %s", folder));
        System.exit(-1);
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }



    public List<String> query1(String warehouseId, String districtId, String startDate, String endDate) throws IOException {
        List<String> customerIDList = new LinkedList<>();
        HConnection connection = HConnectionManager.createConnection(config);
        HTable ordersTable = new HTable(TableName.valueOf(Bytes.toBytes(historyTableName)), connection);
        //TO IMPLEMENT
//        Filter filter1 = new SingleColumnValueFilter(
//                Bytes.toBytes(ordersColumnFamilyName),
//                Bytes.toBytes("O_ENTRY_D"),
//                CompareFilter.CompareOp.GREATER_OR_EQUAL,
//                Bytes.toBytes(startDate)
//        );
        byte[] startKey = getKey(new String[]{warehouseId, districtId, startDate}, new int[]{0, 1, 2});
        byte[] endKey = getKey(new String[]{warehouseId, districtId, endDate}, new int[]{0, 1, 2});

        Scan scan = new Scan(startKey, endKey);
//        PrefixFilter prefixFilter = new PrefixFilter(startAndEndKey);
//        scan.setFilter(prefixFilter);

        ResultScanner resultScanner = ordersTable.getScanner(scan);
        Result res = resultScanner.next();
        while (res != null && !res.isEmpty())
        {
            byte[] warehouseID = res.getValue(Bytes.toBytes(historyColumnFamilyName), Bytes.toBytes("H_W_ID"));
            byte[] districtID = res.getValue(Bytes.toBytes(historyColumnFamilyName), Bytes.toBytes("H_D_ID"));
            byte[] orderDate = res.getValue(Bytes.toBytes(historyColumnFamilyName), Bytes.toBytes("H_DATE"));
            byte[] customerID = res.getValue(Bytes.toBytes(historyColumnFamilyName), Bytes.toBytes("H_C_ID"));

            String whString = new String(warehouseID, "US-ASCII");
            String dsIDString = new String(districtID, "US-ASCII");
            String orderDateString = new String(orderDate, "US-ASCII");
            String customerIDString = new String(customerID, "US-ASCII");
//            System.out.println(whString + " " + dsIDString + " " + orderDateString + " " + customerIDString);

            customerIDList.add(customerIDString);
            res = resultScanner.next();
        }

        return customerIDList;

    }

    public void query2(String warehouseId, String districtId, String customerId, String[] discounts) throws IOException {
        HConnection connection = HConnectionManager.createConnection(config);
        HTable customerTable = new HTable(TableName.valueOf(Bytes.toBytes(customerTableName)),connection);


        for (String discount : discounts)
        {
            Put put = new Put(getKey(
                    new String[] {warehouseId, districtId, customerId},
                    new int[] {0,1,2}
            ));

            put.add(
                    Bytes.toBytes(customerColumnFamilyName),
                    Bytes.toBytes("C_DISCOUNT"),
                    Bytes.toBytes(discount)
            );
            customerTable.put(put);
        }


        System.exit(0);
    }

    public String[] query3(String warehouseId, String districtId, String customerId) throws IOException {
        List<String> discounts = new LinkedList<>();
        HConnection connection = HConnectionManager.createConnection(config);
        HTable customerTable = new HTable(TableName.valueOf(Bytes.toBytes(customerTableName)), connection);
        Get get = new Get(
                getKey(
                        new String[]{warehouseId, districtId, customerId},
                        new int[]{0, 1, 2})
        );

        get.addColumn(Bytes.toBytes(customerColumnFamilyName),Bytes.toBytes("C_DISCOUNT"));
        get.setMaxVersions(4);

        Result res = customerTable.get(get);
        Cell current;
        while(res != null && !res.isEmpty() && res.advance())
        {
            current = res.current();

            byte[] discountValue = CellUtil.cloneValue(current);
            discounts.add(new String(discountValue, "US-ASCII"));
        }

        String[] result = new String[discounts.size()];
        discounts.toArray(result);
        return result;
    }

    public int query4(String warehouseId, String[] districtIds) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return 0;
    }

    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTables, loadTables, query1, query2, query3, query4], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTables: csvsFolder.\n " +
                    "\tb) If query1: warehouseId districtId startDate endData.\n  " +
                    "\tc) If query2: warehouseId districtId customerId listOfDiscounts.\n  " +
                    "\td) If query3: warehouseId districtId customerId.\n  " +
                    "\te) If query4: warehouseId listOfdistrictId.\n  ");
            System.exit(-1);
        }
        HBaseTPCC hBaseTPCC = new HBaseTPCC(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLES")){
            hBaseTPCC.createTPCCTables();
        }
        else if(args[1].toUpperCase().equals("LOADTABLES")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseTPCC.loadTables(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) warehouseId 4) districtId 5) startDate 6) endData");
                System.exit(-1);
            }

            List<String> customerIds = hBaseTPCC.query1(args[2], args[3], args[4], args[5]);
            System.out.println("There are "+customerIds.size()+" customers that order products from warehouse "+args[2]+" of district "+args[3]+" during after the "+args[4]+" and before the "+args[5]+".");
            System.out.println("The list of customers is: "+Arrays.toString(customerIds.toArray(new String[customerIds.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=6){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) warehouseId 4) districtId 5) customerId 6) listOfDiscounts");
                System.exit(-1);
            }
            hBaseTPCC.query2(args[2], args[3], args[4], args[5].split(","));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=5){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) districtId 5) customerId");
                System.exit(-1);
            }
            String[] discounts = hBaseTPCC.query3(args[2], args[3], args[4]);
            System.out.println("The last 4 discounts obtained from Customer "+args[4]+" of warehouse "+args[2]+" of district "+args[3]+" are: "+Arrays.toString(discounts));
        }
        else if(args[1].toUpperCase().equals("QUERY4")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) warehouseId 4) listOfDistrictIds");
                System.exit(-1);
            }
            System.out.println("There are "+hBaseTPCC.query4(args[2], args[3].split(","))+" customers that belong to warehouse "+args[2]+" of districts "+args[3]+".");
        }
        else{
            System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables, query1, query2, query3, query4], 3)Extra parameters for loadTables and queries:" +
                    "a) If loadTables: csvsFolder." +
                    "b) If query1: warehouseId districtId startDate endData" +
                    "c) If query2: warehouseId districtId customerId listOfDiscounts" +
                    "d) If query3: warehouseId districtId customerId " +
                    "e) If query4: warehouseId listOfdistrictId");
            System.exit(-1);
        }

    }

    void createTableWrapper(String tableNameString, String[] columnFamilyNameString, Integer[] maxVersions) throws IOException {
        byte[] tableName = Bytes.toBytes(tableNameString);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(int i =0 ; i < columnFamilyNameString.length; i++)
        {
            byte[] columnFamilyName = Bytes.toBytes(columnFamilyNameString[i]);
            HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(
                    columnFamilyName
            );
            columnFamilyDescriptor.setMaxVersions(maxVersions[i]);
            tableDescriptor.addFamily(columnFamilyDescriptor);
        }
        try {
            hBaseAdmin.createTable(tableDescriptor);
        }
        catch (TableExistsException ex)
        {
            System.err.println(String.format("Table %s already exists, skipping.", tableNameString));
        }

    }

    void loadTableWrapper(
            HConnection connection,
            String tableName,
            String columnFamilyName,
            String folderName,
            String fileName,
            String[] columnNames,
            int[] keyIndexes
            ) throws IOException {

        //first we have to read the file
        File inputCsv = new File(Paths.get(folderName,fileName).toString());
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputCsv));


        List<Put> putList = new LinkedList<>();

        String line;
        while ((line = bufferedReader.readLine()) != null) {


            String [] tokens = line.split(",");
            byte[] rowKey = getKey(tokens,keyIndexes);
            Put put = new Put(rowKey);
            for (int i=0; i<columnNames.length ; i++)
            {
                put.add(
                        Bytes.toBytes(columnFamilyName),
                        Bytes.toBytes(columnNames[i]),
                        Bytes.toBytes(tokens[i])
                );
            }
            putList.add(put);
        }

        HTable hTable = new HTable(TableName.valueOf(Bytes.toBytes(tableName)), connection);
        hTable.put(putList);

    }



}
