package com.hsbc.stratcomp.fi.transform;

import org.springframework.beans.factory.annotation.Autowired;
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

public class IQServiceTransform {

    @Autowired
    private ResourceLoader resourceLoader;

    public void populatePLSTableFromTransformedXML(String transformedXmlPath, String location, String businessDate) {
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
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(transformedXmlPath);
            document.getDocumentElement().normalize();

            // Get all MoneyMarketQuotes nodes
            NodeList moneyMarketQuotesNodes = document.getElementsByTagName("MoneyMarketQuotes");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date parsedDate = sdf.parse(businessDate);
            String prevDate = sdf.format(new Date(parsedDate.getTime() - 86400000L)); // Previous date

            String sql = "INSERT INTO mkt_yeild_pc (Location, System_location, Application, Curvetype, Asofdate, " +
                         "Prevdate, Curveid, Mkttype, Term, Todate, Rate, Spread, Import_date, Commodity1, Commodity2) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            preparedStatement = connection.prepareStatement(sql);

            for (int i = 0; i < moneyMarketQuotesNodes.getLength(); i++) {
                Node moneyMarketQuotesNode = moneyMarketQuotesNodes.item(i);

                // Skip if this <MoneyMarketQuotes> is inside a <SwapCurve>
                if (isChildOfElement(moneyMarketQuotesNode, "SwapCurve")) {
                    continue; // Ignore this MoneyMarketQuotes
                }

                if (moneyMarketQuotesNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element moneyMarketQuotesElement = (Element) moneyMarketQuotesNode;
                    String ccy = moneyMarketQuotesElement.getAttribute("ccy");
                    String rateFixingIndex = moneyMarketQuotesElement.getAttribute("rateFixingindex");

                    NodeList quoteNodes = moneyMarketQuotesElement.getElementsByTagName("Quote");

                    for (int j = 0; j < quoteNodes.getLength(); j++) {
                        Element quoteElement = (Element) quoteNodes.item(j);
                        String tenor = quoteElement.getAttribute("tenor");
                        String midRate = quoteElement.getAttribute("midRate");

                        // Determine mkttype based on tenor
                        String mkttype = determineMkttype(tenor);

                        // Set values for the prepared statement
                        preparedStatement.setString(1, location); // Location
                        preparedStatement.setString(2, "PARIS"); // System_location
                        preparedStatement.setString(3, "SUMMIT"); // Application
                        preparedStatement.setString(4, "YCURVE"); // Curvetype
                        preparedStatement.setString(5, businessDate); // Asofdate
                        preparedStatement.setString(6, prevDate); // Prevdate
                        preparedStatement.setString(7, "MSSEOD"); // Curveid
                        preparedStatement.setString(8, mkttype); // Mkttype
                        preparedStatement.setString(9, tenor); // Term
                        preparedStatement.setNull(10, java.sql.Types.VARCHAR); // Todate (null)
                        preparedStatement.setDouble(11, Double.parseDouble(midRate)); // Rate
                        preparedStatement.setDouble(12, 0.0); // Spread (always 0.0)
                        preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis())); // Import_date
                        preparedStatement.setString(14, ccy); // Commodity1 (currency)
                        preparedStatement.setString(15, rateFixingIndex); // Commodity2 (rateFixingindex)

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

    // Method to determine the mkttype based on the tenor
    private String determineMkttype(String tenor) {
        try {
            if (tenor.matches("\\d+[DWMY]")) { // e.g., "1Y", "3M", "10D"
                int numericTenor = Integer.parseInt(tenor.replaceAll("[^\\d]", ""));
                char tenorUnit = tenor.charAt(tenor.length() - 1);

                if (tenorUnit == 'Y' && numericTenor < 1) {
                    return "MM";
                } else if (tenorUnit == 'M' || tenorUnit == 'W' || tenorUnit == 'D') {
                    return "MM";
                } else if (tenor.matches("[A-Z]{3}\\d{2}")) { // e.g., "DEC25"
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

    // Helper method to check if a node is a child of a specific element
    private boolean isChildOfElement(Node node, String parentTagName) {
        Node parentNode = node.getParentNode();
        while (parentNode != null) {
            if (parentNode.getNodeType() == Node.ELEMENT_NODE && parentNode.getNodeName().equals(parentTagName)) {
                return true;
            }
            parentNode = parentNode.getParentNode();
        }
        return false;
    }
}
