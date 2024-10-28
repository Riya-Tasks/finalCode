// Add a logger to your class:

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Initialize the logger:

private static final Logger logger = LoggerFactory.getLogger(IQServiceTransform.class);


public void populatePLSTableFromTransformedXML(String location, String businessDate) {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    String prevDate = null;

    try {
        // Oracle DB connection setup
        logger.info("Setting up Oracle DB connection.");
        String jdbcUrl = "jdbc:oracle:thin:@//host:port/service";
        String username = "your_db_username";
        String password = "your_db_password";
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false); // for batch processing

        // Retrieve the previous business date from the database
        String prevDateQuery = "SELECT GETPREVIOUSBUSINESSDAY(?, 'PARIS', ?) FROM DUAL";
        try (PreparedStatement prevDateStatement = connection.prepareStatement(prevDateQuery)) {
            prevDateStatement.setString(1, location);
            prevDateStatement.setString(2, businessDate);
            logger.info("Executing query to retrieve previous business date with location: {} and businessDate: {}", location, businessDate);

            try (ResultSet rs = prevDateStatement.executeQuery()) {
                if (rs.next()) {
                    prevDate = rs.getString(1);
                    logger.info("Previous business date retrieved: {}", prevDate);
                }
            }
        }

        if (prevDate == null) {
            logger.error("Failed to retrieve previous business date.");
            throw new Exception("Failed to retrieve previous business date.");
        }
        
        // Parse the transformed XML file
        String xdsFilePath = System.getProperty("user.dir") + "/xds/transformedXML.xml";
        logger.info("Parsing transformed XML file at path: {}", xdsFilePath);
        File xmlFile = new File(xdsFilePath);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        document.getDocumentElement().normalize();

        // Prepare date formats
        logger.info("Processing business date: {} and previous date: {}", businessDate, prevDate);

        // SQL template for prepared statement
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
        logger.info("Executing batch insert into mkt_yeild_pc.");
        preparedStatement.executeBatch();
        connection.commit();
        logger.info("Data successfully inserted into mkt_yeild_pc.");

    } catch (Exception e) {
        logger.error("An error occurred: ", e);
        if (connection != null) {
            try {
                connection.rollback();
                logger.info("Transaction rolled back.");
            } catch (Exception rollbackEx) {
                logger.error("Error during rollback: ", rollbackEx);
            }
        }
    } finally {
        try {
            if (preparedStatement != null) preparedStatement.close();
            if (connection != null) connection.close();
        } catch (Exception ex) {
            logger.error("Error closing resources: ", ex);
        }
    }
}
 // processMoneyMarketQuotes

private void processMoneyMarketQuotes(Document document, PreparedStatement preparedStatement,
                                      String location, String businessDate, String prevDate) throws Exception {
    NodeList moneyMarketQuotesNodes = document.getElementsByTagName("MoneyMarketQuotes");

    for (int i = 0; i < moneyMarketQuotesNodes.getLength(); i++) {
        Node moneyMarketQuotesNode = moneyMarketQuotesNodes.item(i);

        if (isChildOfElement(moneyMarketQuotesNode, "SwapCurve")) {
            logger.info("Skipping MoneyMarketQuotes inside SwapCurve.");
            continue;
        }

        if (moneyMarketQuotesNode.getNodeType() == Node.ELEMENT_NODE) {
            Element moneyMarketQuotesElement = (Element) moneyMarketQuotesNode;
            String ccy = moneyMarketQuotesElement.getAttribute("ccy");
            String rateFixingIndex = moneyMarketQuotesElement.getAttribute("rateFixingindex");

            logger.info("Processing MoneyMarketQuotes for ccy: {} and rateFixingIndex: {}", ccy, rateFixingIndex);

            NodeList quoteNodes = moneyMarketQuotesElement.getElementsByTagName("Quote");

            for (int j = 0; j < quoteNodes.getLength(); j++) {
                Element quoteElement = (Element) quoteNodes.item(j);
                String tenor = quoteElement.getAttribute("tenor");
                String midRate = quoteElement.getAttribute("midRate");

                logger.info("Inserting Quote with tenor: {} and midRate: {}", tenor, midRate);

                String mkttype = determineMkttypeMoneyMarketQuotes(tenor);

                preparedStatement.setString(1, location);
                preparedStatement.setString(2, "PARIS");
                preparedStatement.setString(3, "SUMMIT");
                preparedStatement.setString(4, "YCURVE");
                preparedStatement.setString(5, businessDate);
                preparedStatement.setString(6, prevDate);
                preparedStatement.setString(7, "MSSEOD");
                preparedStatement.setString(8, mkttype);
                preparedStatement.setString(9, tenor);
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

// processSwapRates

// Sub-function to process SwapRates nodes
private void processSwapRates(Document document, PreparedStatement preparedStatement,
                              String location, String businessDate, String prevDate) throws Exception {
    NodeList swapRatesNodes = document.getElementsByTagName("SwapRates");
    
    for (int i = 0; i < swapRatesNodes.getLength(); i++) {
        Node swapRatesNode = swapRatesNodes.item(i);

        if (isChildOfElement(swapRatesNode, "SwapCurve")) {
            logger.info("Skipping SwapRates inside SwapCurve.");
            continue;
        }

        if (swapRatesNode.getNodeType() == Node.ELEMENT_NODE) {
            Element swapRatesElement = (Element) swapRatesNode;
            String ccy = swapRatesElement.getAttribute("ccy");
            String rateFixingIndex = swapRatesElement.getAttribute("rateFixingIndex");

            logger.info("Processing SwapRates for ccy: {} and rateFixingIndex: {}", ccy, rateFixingIndex);

            NodeList quoteNodes = swapRatesElement.getElementsByTagName("Quote");

            for (int j = 0; j < quoteNodes.getLength(); j++) {
                Element quoteElement = (Element) quoteNodes.item(j);
                String term = quoteElement.getAttribute("term");
                String midRate = quoteElement.getAttribute("midRate");

                logger.info("Inserting Quote with term: {} and midRate: {}", term, midRate);

                String mkttype = determineMkttypeSwapRates(term);

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


// processInflationSwap

// Sub-function to process InflationSwap nodes
private void processInflationSwap(Document document, PreparedStatement preparedStatement,
                                  String location, String businessDate, String prevDate) throws Exception {
    NodeList inflationSwapNodes = document.getElementsByTagName("InflationSwap");
    
    for (int i = 0; i < inflationSwapNodes.getLength(); i++) {
        Node inflationSwapNode = inflationSwapNodes.item(i);

        if (isChildOfElement(inflationSwapNode, "InflationCurve")) {
            logger.info("Skipping InflationSwap inside InflationCurve.");
            continue;
        }

        if (inflationSwapNode.getNodeType() == Node.ELEMENT_NODE) {
            Element inflationSwapElement = (Element) inflationSwapNode;
            String ccy = inflationSwapElement.getAttribute("ccy");
            String indexType = inflationSwapElement.getAttribute("indexType");

            logger.info("Processing InflationSwap for ccy: {} and indexType: {}", ccy, indexType);

            NodeList elementNodes = inflationSwapElement.getElementsByTagName("Element");

            for (int j = 0; j < elementNodes.getLength(); j++) {
                Element element = (Element) elementNodes.item(j);
                String maturity = element.getAttribute("maturity");
                String rate = element.getTextContent();

                logger.info("Inserting Element with maturity: {} and rate: {}", maturity, rate);

                String mkttype = determineMkttypeInflationSwap(maturity);

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
                preparedStatement.setDouble(11, Double.parseDouble(rate));
                preparedStatement.setDouble(12, 0.0);
                preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis()));
                preparedStatement.setString(14, ccy);
                preparedStatement.setString(15, indexType);

                preparedStatement.addBatch();
            }
        }
    }
}

// processSpreadCurve

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

            logger.info("Processing SpreadCurve for ccy: {} and rateFixingIndex: {}", ccy, rateFixingIndex);

            NodeList quoteNodes = spreadCurveElement.getElementsByTagName("Quote");

            for (int j = 0; j < quoteNodes.getLength(); j++) {
                Element quoteElement = (Element) quoteNodes.item(j);
                String tenor = quoteElement.getAttribute("tenor");
                String spread = quoteElement.getAttribute("spread");

                logger.info("Inserting Quote with tenor: {} and spread: {}", tenor, spread);

                String mkttype = determineMkttypeSpreadCurve(tenor);

                preparedStatement.setString(1, location);
                preparedStatement.setString(2, "PARIS");
                preparedStatement.setString(3, "SUMMIT");
                preparedStatement.setString(4, "YCURVE");
                preparedStatement.setString(5, businessDate);
                preparedStatement.setString(6, prevDate);
                preparedStatement.setString(7, "MSSEOD");
                preparedStatement.setString(8, mkttype);
                preparedStatement.setString(9, tenor);
                preparedStatement.setNull(10, java.sql.Types.VARCHAR);
                preparedStatement.setDouble(11, Double.parseDouble(spread));
                preparedStatement.setDouble(12, 0.0);
                preparedStatement.setDate(13, new java.sql.Date(System.currentTimeMillis()));
                preparedStatement.setString(14, ccy);
                preparedStatement.setString(15, rateFixingIndex);

                preparedStatement.addBatch();
            }
        }
    }
}


