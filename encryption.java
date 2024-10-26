import org.jasypt.util.text.AES256TextEncryptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;

@Service
public class PLSConnect {

    @Autowired
    private Environment env;

    public Connection getPLSConnection(String database) throws Exception {
        Connection conn = null;
        try {
            // JDBC URL construction
            String sqlConnString = "jdbc:oracle:thin:@" + env.getProperty("PLS." + database + ".HOSTNAME") + ":" +
                                   env.getProperty("PLS." + database + ".PORT") + "/" +
                                   env.getProperty("PLS." + database + ".SID");

            // Decrypt the encrypted password
            String decryptedPassword = decryptPassword(env.getProperty("ENCRYPTED_DB_PASSWORD"));

            // Get the connection
            conn = DriverManager.getConnection(sqlConnString, env.getProperty("PLS." + database + ".USERNAME"), decryptedPassword);
        } catch (Exception e) {
            throw new Exception("Failed to connect to the database", e);
        }
        return conn;
    }

    private String decryptPassword(String encryptedPassword) {
        AES256TextEncryptor textEncryptor = new AES256TextEncryptor();
        textEncryptor.setPassword(env.getProperty("JASYPT_ENCRYPTOR_PASSWORD"));
        return textEncryptor.decrypt(encryptedPassword);
    }
}
