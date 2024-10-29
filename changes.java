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

                // Check if tenor is missing
                String term = tenor;
                String toDate = null;
                if (tenor.isEmpty()) {
                    term = quoteElement.getAttribute("startDate");  // Set tenor to startDate if tenor is missing
                    toDate = quoteElement.getAttribute("endDate");   // Set toDate to endDate
                }

                String mkttype = determineMkttypeMoneyMarketQuotes(term);

                // Set values for the prepared statement
                preparedStatement.setString(1, location); // Location
                preparedStatement.setString(2, "PARIS"); // System_location
                preparedStatement.setString(3, "SUMMIT"); // Application
                preparedStatement.setString(4, "YCURVE"); // Curvetype
                preparedStatement.setString(5, businessDate); // Asofdate
                preparedStatement.setString(6, prevDate); // Prevdate
                preparedStatement.setString(7, "MSSEOD"); // Curveid
                preparedStatement.setString(8, mkttype); // Mkttype
                preparedStatement.setString(9, term); // Term
                if (toDate != null) {
                    preparedStatement.setString(10, toDate); // Todate if tenor is missing
                } else {
                    preparedStatement.setNull(10, java.sql.Types.VARCHAR); // Todate (null)
                }
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
