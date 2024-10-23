package com.hsbc.stratcomp.fi.transform;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class IQServiceTransform {

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private FileUtility fileUtility;

    @Autowired
    private TransformCurves transformCurves;

    public void populatePLSTableFromTransformedXML(String location, String businessDate) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            // Oracle DB connection setup
            String jdbcUrl = "jdbc:oracle:thin:@//host:port/service";
            String username = "your_db_username";
            String password = "your_db_password";
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(false); // for batch processing

            // Parse the transformed XML file

	    String xdsFilePath = System.getProperty("user.dir") +"/xds/transformedXML.xml";
            File xmlFile = new File(xdsFilePath);

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(xmlFile);
            document.getDocumentElement().normalize();

            NodeList spreadCurveNodes = document.getElementsByTagName("SpreadCurve");

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date parsedDate = sdf.parse(businessDate);
            String prevDate = sdf.format(new Date(parsedDate.getTime() - 86400000L)); // Previous date (subtracting 1 day)

            String sql = "INSERT INTO mkt_yeild_pc (Location, System_location, Application, Curvetype, Asofdate, " +
                         "Prevdate, Curveid, Mkttype, Term, Todate, Rate, Spread, Import_date, Commodity1, Commodity2) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            preparedStatement = connection.prepareStatement(sql);

            for (int i = 0; i < spreadCurveNodes.getLength(); i++) {
                Node spreadCurveNode = spreadCurveNodes.item(i);

                if (spreadCurveNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element spreadCurveElement = (Element) spreadCurveNode;
                    String ccy = spreadCurveElement.getAttribute("ccy");
                    String rateFixingIndex = spreadCurveElement.getAttribute("rateFixingIndex");

                    NodeList quoteNodes = spreadCurveElement.getElementsByTagName("Quote");

                    for (int j = 0; j < quoteNodes.getLength(); j++) {
                        Element quoteElement = (Element) quoteNodes.item(j);
                        String term = quoteElement.getAttribute("term");
                        String midRate = quoteElement.getAttribute("midRate");

                        // Determine mkttype based on term
                        String mkttype = determineMkttype(term);

                        // Set values for the prepared statement
                        preparedStatement.setString(1, location);
                        preparedStatement.setString(2, "PARIS"); // System_location
                        preparedStatement.setString(3, "SUMMIT"); // Application
                        preparedStatement.setString(4, "YCURVE"); // Curvetype
                        preparedStatement.setString(5, businessDate); // Asofdate
                        preparedStatement.setString(6, prevDate); // Prevdate
                        preparedStatement.setString(7, "MSSEOD"); // Curveid
                        preparedStatement.setString(8, mkttype); // Mkttype
                        preparedStatement.setString(9, term); // Term
                        preparedStatement.setNull(10, java.sql.Types.VARCHAR); // Todate (null)
                        preparedStatement.setDouble(11, Double.parseDouble(midRate)); // Rate
                        preparedStatement.setDouble(12, 0.0); // Spread (always 0.0)
                        preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis())); // Import_date
                        preparedStatement.setString(14, ccy); // Commodity1 (currency)
                        preparedStatement.setString(15, rateFixingIndex); // Commodity2 (rateFixingIndex)

                        // Add to batch
                        preparedStatement.addBatch();
                    }
                }
            }

            // Execute batch insert
            preparedStatement.executeBatch();
            connection.commit();
            System.out.println("Data successfully inserted into mkt_yeild_pc.");

        } catch (Exception e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (Exception rollbackEx) {
                    rollbackEx.printStackTrace();
                }
            }
        } finally {
            try {
                if (preparedStatement != null) preparedStatement.close();
                if (connection != null) connection.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    // Method to determine the mkttype based on the term
    private String determineMkttype(String term) {
        try {
            if (term.matches("\\d+[DWMY]")) { // e.g., "1Y", "3M", "10D"
                int numericTerm = Integer.parseInt(term.replaceAll("[^\\d]", ""));
                char termUnit = term.charAt(term.length() - 1);

                // MM if the term is less than 1 year (Y)
                if (termUnit == 'Y' && numericTerm < 1) {
                    return "MM";
                } else if (termUnit == 'M' || termUnit == 'W' || termUnit == 'D') {
                    return "MM";
                } else if (term.matches("[A-Z]{3}\\d{2}")) { // e.g., "DEC25"
                    return "FUT";
                } else {
                    return "AIC";
                }
            } else {
                return "AIC";
            }
        } catch (Exception e) {
            return "AIC"; // Default to AIC in case of any error
        }
    }
}
