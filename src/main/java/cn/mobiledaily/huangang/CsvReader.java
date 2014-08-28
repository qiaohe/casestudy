package cn.mobiledaily.huangang;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

/**
 * Created by dell on 14-8-28.
 */
public class CsvReader {
    private final String csvFile;
    private static final String INSERT_SQL = "INSERT INTO TradeInfo(shipper,shipperaddr1,shipperaddr2,shipperaddr3," +
            "consignee,consigneeaddr1,consigneeaddr2,consigneeaddr3,notifypartyname,notifypartyaddr1,notifypartyaddr2" +
            ",notifypartyaddr3,[weight],weightunit,arrivaldate,pieces,piecesunit,measure,measureunit,origin,vessel," +
            "goodsdesc,carrier,carriersimple,voyage) " +
            "values(''{0}'',''{1}'',''{2}'',''{3}'',''{4}'',''{5}'',''{6}'',''{7}'',''{8}'',''{9}'',''{10}'',''{11}''," +
            "''{12}'',''{13}'',''{14}'',''{15}'',''{16}'',''{17}'',''{18}'',''{19}'',''{20}'',''{21}'',''{22}''," +
            " ''{23}'',''{24}'')";
    private static final CSVFormat CSV_FORMAT = CSVFormat.newFormat('|').withSkipHeaderRecord(true).withHeader("Bill_Of_Lading_Nbr", "Arrival_Date", "House Master", "Master B L", "Carrier_Code", "Master_Carrier", "Vessel_Name", "Vessel_Code", "Voyage_Number", "Mode_Transport", "Foreign_Port_Lading", "Place_Receipt", "DP_Of_Unlading", "Inbond_Entry_Type", "US_Dist_Port", "Foreign_Distribution_Port", "FROB", "Weight", "Weight_Unit", "Manifest_Qty", "Manifest_Units", "Measurement", "Measurement_Unit", "Estimated Arrival", "Actual Arrival", "Piece_Count", "Cargo_Description", "Shipper_Name", "Shipper_Addr_1", "Shipper_Addr_2", "Shipper_Addr_3", "Shipper_Addr_4", "Consignee_Name", "Consignee_Addr_1", "Consignee_Addr_2", "Consignee_Addr_3", "Consignee_Addr_4", "Notify_Name", "Notify_Addr_1", "Notify_Addr_2", "Notify_Addr_3", "Notify_Addr_4", "Notify_2_Name", "Notify_2_Addr_1", "Notify_2_Addr_2", "Notify_2_Addr_3", "Notify_2_Addr_4", "Container_Number", "Container_Marks", "Container_Length", "Container_Height", "Container_Width")
            .withSkipHeaderRecord(true).withRecordSeparator('|').withIgnoreEmptyLines(false);

    public CsvReader(String csvFile) {
        this.csvFile = csvFile;
    }

    public Connection getConnection() {
        String url = "jdbc:jtds:sqlserver://211.149.208.174:1433/TendReport";
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            return DriverManager.getConnection(url, "sa", "111");
        } catch (Exception e) {
            System.err.println("Cannot connect to database server");
            e.printStackTrace();
        }
        return null;
    }

    private String replace(String value) {
        return value.replace("'", " ");
    }

    public void insertTrades() throws IOException, SQLException {
        Reader in = new InputStreamReader(new FileInputStream(csvFile), "Unicode");
        Iterable<CSVRecord> records = CSV_FORMAT.parse(in);
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        final int batchSize = 1000;
        int count = 0;
        for (CSVRecord record : records) {
            final String query = MessageFormat.format(INSERT_SQL, record.get("Shipper_Name"), record.get("Shipper_Addr_1"), record.get("Shipper_Addr_2"), record.get("Shipper_Addr_3"),
                    replace(record.get("Consignee_Name")), replace(record.get("Consignee_Addr_1")), replace(record.get("Consignee_Addr_2")),
                    replace(record.get("Consignee_Addr_3")), replace(record.get("Notify_Name"))
                    , replace(record.get("Notify_Addr_1")), replace(record.get("Notify_Addr_2")), replace(record.get("Notify_Addr_3")), replace(record.get("Weight"))
                    , replace(record.get("Weight_Unit")), replace(record.get("Arrival_Date")), replace(record.get("Manifest_Qty")), replace(record.get("Manifest_Units"))
                    , replace(record.get("Measurement")), replace(record.get("Measurement_Unit")), replace(record.get("Place_Receipt")), replace(record.get("Vessel_Name")),
                    replace(record.get("Cargo_Description")), replace(record.get("Master_Carrier")), replace(record.get("Carrier_Code")), replace(record.get("Voyage_Number")));
            try {
                statement.addBatch(query);
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
            if (++count % batchSize == 0) {
                statement.executeBatch();
            }
        }
        statement.executeBatch();
        statement.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException, SQLException {
        CsvReader reader = new CsvReader("F:\\data\\2013_DataSample.csv");
        reader.insertTrades();
    }
}
