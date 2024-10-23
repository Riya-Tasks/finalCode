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

            // Prepare date formats
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date parsedDate = sdf.parse(businessDate);
            String prevDate = sdf.format(new Date(parsedDate.getTime() - 86400000L)); // Previous date

            // Common SQL for all nodes
            String sql = "INSERT INTO mkt_yeild_pc (Location, System_location, Application, Curvetype, Asofdate, " +
                         "Prevdate, Curveid, Mkttype, Term, Todate, Rate, Spread, Import_date, Commodity1, Commodity2) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            preparedStatement = connection.prepareStatement(sql);

            // Process MoneyMarketQuotes
            processMoneyMarketQuotes(document, preparedStatement, location, businessDate, prevDate);

            // Process SwapRates
            processSwapRates(document, preparedStatement, location, businessDate, prevDate);

            // Process InflationSwap
            processInflationSwap(document, preparedStatement, location, businessDate, prevDate);

            // Process SpreadCurve
            processSpreadCurve(document, preparedStatement, location, businessDate, prevDate);

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

    // Sub-function to process MoneyMarketQuotes nodes
    private void processMoneyMarketQuotes(Document document, PreparedStatement preparedStatement,
                                          String location, String businessDate, String prevDate) throws Exception {
        NodeList moneyMarketQuotesNodes = document.getElementsByTagName("MoneyMarketQuotes");

        for (int i = 0; i < moneyMarketQuotesNodes.getLength(); i++) {
            Node moneyMarketQuotesNode = moneyMarketQuotesNodes.item(i);

            if (isChildOfElement(moneyMarketQuotesNode, "SwapCurve")) {
                continue; // Ignore MoneyMarketQuotes inside SwapCurve
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

                    String mkttype = determineMkttypeMoneyMarketQuotes(tenor);

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
                    preparedStatement.setString(14, ccy); // Commodity1
                    preparedStatement.setString(15, rateFixingIndex); // Commodity2

                    preparedStatement.addBatch();
                }
            }
        }
    }

    // Sub-function to process SwapRates nodes
    private void processSwapRates(Document document, PreparedStatement preparedStatement,
                                  String location, String businessDate, String prevDate) throws Exception {
        NodeList swapRatesNodes = document.getElementsByTagName("SwapRates");

        for (int i = 0; i < swapRatesNodes.getLength(); i++) {
            Node swapRatesNode = swapRatesNodes.item(i);

            if (isChildOfElement(swapRatesNode, "SwapCurve")) {
                continue; // Ignore SwapRates inside SwapCurve
            }

            if (swapRatesNode.getNodeType() == Node.ELEMENT_NODE) {
                Element swapRatesElement = (Element) swapRatesNode;
                String ccy = swapRatesElement.getAttribute("ccy");
                String rateFixingIndex = swapRatesElement.getAttribute("rateFixingIndex");

                NodeList quoteNodes = swapRatesElement.getElementsByTagName("Quote");

                for (int j = 0; j < quoteNodes.getLength(); j++) {
                    Element quoteElement = (Element) quoteNodes.item(j);
                    String term = quoteElement.getAttribute("term");
                    String midRate = quoteElement.getAttribute("midRate");

                    String mkttype = determineMkttypeSwapRates(term);

                    // Set values for the prepared statement
                    preparedStatement.setString(1, location);
                    preparedStatement.setString(2, "PARIS");
                    preparedStatement.setString(3, "SUMMIT");
                    preparedStatement.setString(4, "YCURVE");
                    preparedStatement.setString(5, businessDate);
                    preparedStatement.setString(6, prevDate);
                    preparedStatement.setString(7, "MSSEOD");
                    preparedStatement.setString(8, mkttype);
                    preparedStatement.setString(9, term);
                    preparedStatement.setNull(10, java.sql.Types.VARCHAR);
                    preparedStatement.setDouble(11, Double.parseDouble(midRate));
                    preparedStatement.setDouble(12, 0.0);
                    preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis()));
                    preparedStatement.setString(14, ccy);
                    preparedStatement.setString(15, rateFixingIndex);

                    preparedStatement.addBatch();
                }
            }
        }
    }

    // Sub-function to process InflationSwap nodes
    private void processInflationSwap(Document document, PreparedStatement preparedStatement,
                                      String location, String businessDate, String prevDate) throws Exception {
        NodeList inflationSwapNodes = document.getElementsByTagName("InflationSwap");

        for (int i = 0; i < inflationSwapNodes.getLength(); i++) {
            Node inflationSwapNode = inflationSwapNodes.item(i);

            // Skip if this <InflationSwap> is inside an <InflationCurve>
                if (isChildOfElement(inflationSwapNode, "InflationCurve")) {
                    continue; // Ignore this InflationSwap
                }

            if (inflationSwapNode.getNodeType() == Node.ELEMENT_NODE) {
                Element inflationSwapElement = (Element) inflationSwapNode;
                String ccy = inflationSwapElement.getAttribute("ccy");
                String rateFixingIndex = inflationSwapElement.getAttribute("indexType");

                NodeList quoteNodes = inflationSwapElement.getElementsByTagName("Element");

                for (int j = 0; j < elementNodes.getLength(); j++) {
                        Element element = (Element) elementNodes.item(j);
                        String maturity = element.getAttribute("maturity");
                        String rate = element.getTextContent();

                    String mkttype = determineMkttypeInflationSwap(maturity);

                    // Set values for the prepared statement
                    preparedStatement.setString(1, location);
                    preparedStatement.setString(2, "PARIS");
                    preparedStatement.setString(3, "SUMMIT");
                    preparedStatement.setString(4, "YCURVE");
                    preparedStatement.setString(5, businessDate);
                    preparedStatement.setString(6, prevDate);
                    preparedStatement.setString(7, "MSSEOD");
                    preparedStatement.setString(8, mkttype);
                    preparedStatement.setString(9, maturity);
                    preparedStatement.setNull(10, java.sql.Types.VARCHAR);
                    preparedStatement.setDouble(11, Double.parseDouble(midRate));
                    preparedStatement.setDouble(12, 0.0);
                    preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis()));
                    preparedStatement.setString(14, ccy);
                    preparedStatement.setString(15, indexType);

                    preparedStatement.addBatch();
                }
            }
        }
    }

    // Sub-function to process SpreadCurve nodes
    private void processSpreadCurve(Document document, PreparedStatement preparedStatement,
                                    String location, String businessDate, String prevDate) throws Exception {
        NodeList spreadCurveNodes = document.getElementsByTagName("SpreadCurve");

        for (int i = 0; i < spreadCurveNodes.getLength(); i++) {
            Node spreadCurveNode = spreadCurveNodes.item(i);

            if (spreadCurveNode.getNodeType() == Node.ELEMENT_NODE) {
                Element spreadCurveElement = (Element) spreadCurveNode;
                String ccy = spreadCurveElement.getAttribute("ccy");
                String rateFixingIndex = spreadCurveElement.getAttribute("rateFixingIndex");

                NodeList spreadNodes = spreadCurveElement.getElementsByTagName("Quote");

                for (int j = 0; j < spreadNodes.getLength(); j++) {
                    Element spreadElement = (Element) spreadNodes.item(j);
                    String term = spreadElement.getAttribute("term");
                    String spreadValue = spreadElement.getAttribute("midRate");

                    String mkttype = determineMkttypeSpreadCurve(term);

                    // Set values for the prepared statement
                    preparedStatement.setString(1, location);
                    preparedStatement.setString(2, "PARIS");
                    preparedStatement.setString(3, "SUMMIT");
                    preparedStatement.setString(4, "YCURVE");
                    preparedStatement.setString(5, businessDate);
                    preparedStatement.setString(6, prevDate);
                    preparedStatement.setString(7, "MSSEOD");
                    preparedStatement.setString(8, mkttype);
                    preparedStatement.setString(9, term);
                    preparedStatement.setNull(10, java.sql.Types.VARCHAR);
                    preparedStatement.setDouble(11, 0.0); // Rate is always 0.0 for SpreadCurve
                    preparedStatement.setDouble(12, Double.parseDouble(spreadValue)); // Spread
                    preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis()));
                    preparedStatement.setString(14, ccy);
                    preparedStatement.setString(15, rateFixingIndex);

                    preparedStatement.addBatch();
                }
            }
        }
    }

    // Helper method to determine mkttype based on term - SPREAD CURVE
    private String determineMkttypeSpreadCurve(String term) {
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

        // Helper method to determine mkttype based on term - Inflation Swap
        private String determineMkttypeInflationSwap(String maturity) {
          try {
              if (maturity.matches("\\d+[DWMY]")) { // e.g., "1Y", "3M", "10D"
                  int numericMaturity = Integer.parseInt(maturity.replaceAll("[^\\d]", ""));
                  char maturityUnit = maturity.charAt(maturity.length() - 1);

                  if (maturityUnit == 'Y' && numericMaturity < 1) {
                      return "MM";
                  } else if (maturityUnit == 'M' || maturityUnit == 'W' || maturityUnit == 'D') {
                      return "MM";
                  } else if (maturity.matches("[A-Z]{3}\\d{2}")) { // e.g., "DEC25"
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

      // Helper method to determine mkttype based on term - MoneyMarketQuotes
      private String determineMkttypeMoneyMarketQuotes(String tenor) {
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

    // Helper method to determine mkttype based on term - SwapRates
    private String determineMkttypeSwapRates(String term) {
        try {
            if (term.matches("\\d+[DWMY]")) { // e.g., "1Y", "3M", "10D"
                int numericTerm = Integer.parseInt(term.replaceAll("[^\\d]", ""));
                char termUnit = term.charAt(term.length() - 1);

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



    // Helper method to check if the current node is a child of a given element name
    private boolean isChildOfElement(Node node, String parentElementName) {
        Node parentNode = node.getParentNode();
        while (parentNode != null) {
            if (parentNode.getNodeType() == Node.ELEMENT_NODE &&
                ((Element) parentNode).getTagName().equals(parentElementName)) {
                return true;
            }
            parentNode = parentNode.getParentNode();
        }
        return false;
    }
}
