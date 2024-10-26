public void populatePLSTableFromTransformedXML(String location, String businessDate) {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    String prevDate = null;

    try {
        // Oracle DB connection setup
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

            try (ResultSet rs = prevDateStatement.executeQuery()) {
                if (rs.next()) {
                    prevDate = rs.getString(1);
                }
            }
        }

        if (prevDate == null) {
            throw new Exception("Failed to retrieve previous business date.");
        }

        // Parse the transformed XML file
        String xdsFilePath = System.getProperty("user.dir") + "/xds/transformedXML.xml";
        File xmlFile = new File(xdsFilePath);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        document.getDocumentElement().normalize();

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
