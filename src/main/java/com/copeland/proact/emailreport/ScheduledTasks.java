package com.copeland.proact.emailreport;

import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.io.*;
import java.sql.*;
import java.util.Properties;

@Component
@Slf4j
public class ScheduledTasks {

    //private static final String CSV_FILE = "/path/to/yourfile.csv";

    //private static final String CSV_FILE = "D:\\Dollar General HVAC Daily Report.csv";


    @Value("${report.hvac.csv.template:D:\\\\Dollar General HVAC Daily Report.csv}")
    private String CSV_FILE;

    @Value("${report.hvac.fromemail:garys.sun@copeland.com}")
    private String fromEmail;

    @Value("${report.hvac.toemail:garys.sun@copeland.com}")
    private String toEmail;
    @Value("${report.smtp.server.host:INETMAIL.EMRSN.NET}")
    private String smtp_server;

    @Autowired
    private DatabaseComponent databaseComponent;

    //@Scheduled(cron = "0 0 7 * * ?")
    @Scheduled(initialDelay = 1000, fixedDelayString = "30000000")
    public void performTask() {
        log.info("Start to export HVAC report......");
        try (Connection conn = databaseComponent.getConnection()) {
            // connect oracle
            log.info("connect oracle......");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select \n" +
                    "store as \"Store\",\n" +
                    "decode(DEG_80_EXCEEDED_1,0,null,DEG_80_EXCEEDED_1) as \"Count of Exceeded 80F Space Temp Alarms\",\n" +
                    "DEG_85_EXCEEDED_1 as \"Count of Exceeded 85F Space Temp Alarms\",\n" +
                    "decode(DEG_55_EXCEEDED_1,0,null,DEG_55_EXCEEDED_1) as \"Count of Below 55F Space Temp Alarms\",\n" +
                    "decode(SPACE_TEMP_NOT_ACHIEVED_1,0,null,SPACE_TEMP_NOT_ACHIEVED_1) as \"Count of Space Temp Not Achieved Alarms\",\n" +
                    "decode(UNIT_RUNNING_AND_NOT_COOLING_1,0,null,UNIT_RUNNING_AND_NOT_COOLING_1) \"Count of Running and Not Cooling Alarms\",\n" +
                    "DEG_80_EXCEEDED_2 as \"Units Exceeded 80F Space Temp Alarms\",\n" +
                    "DEG_85_EXCEEDED_2 as \"Units Exceeded 85F Space Temp Alarms\",\n" +
                    "DEG_55_EXCEEDED_2 as \"Units Below 55F Space Temp Alarms\",\n" +
                    "SPACE_TEMP_NOT_ACHIEVED_2 as \"Units Space Temp Not Achieved\",\n" +
                    "UNIT_RUNNING_AND_NOT_COOLING_2 as \"Units Running and Not Cooling\",\n" +
                    "NUMBER_OF_UNITS_DOWN as \"Number of Units Down\",\n" +
                    "TOTAL_UNITS as \"Total Units\"\n" +
                    "from sf_dg_innovations_new\n" +
                    "where trunc(alarm_monitoring_date) =trunc(sysdate-1) order by store");

            // save csv
            FileWriter fw = new FileWriter(CSV_FILE);
            fw.append("\"Store\"");
            fw.append(',');
            fw.append("\"Count of Exceeded 80F Space Temp Alarms\"");
            fw.append(',');
            fw.append("\"Count of Exceeded 85F Space Temp Alarms\"");
            fw.append(',');
            fw.append("\"Count of Below 55F Space Temp Alarms\"");
            fw.append(',');
            fw.append("\"Count of Space Temp Not Achieved Alarms\"");
            fw.append(',');
            fw.append("\"Count of Running and Not Cooling Alarms\"");
            fw.append(',');
            fw.append("\"Units Exceeded 80F Space Temp Alarms\"");
            fw.append(',');
            fw.append("\"Units Exceeded 85F Space Temp Alarms\"");
            fw.append(',');
            fw.append("\"Units Below 55F Space Temp Alarms\"");
            fw.append(',');
            fw.append("\"Units Space Temp Not Achieved\"");
            fw.append(',');
            fw.append("\"Units Running and Not Cooling\"");
            fw.append(',');
            fw.append("\"Number of Units Down\"");
            fw.append(',');
            fw.append("\"Total Units\"");
            fw.append(',');
            fw.append('\n');
            while (rs.next()) {
                log.info("insert data into csv......");
                fw.append("\"" + (rs.getString(1) != null ? rs.getString(1) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(2) != null ? rs.getString(2) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(3) != null ? rs.getString(3) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(4) != null ? rs.getString(4) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(5) != null ? rs.getString(5) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(6) != null ? rs.getString(6) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(7) != null ? rs.getString(7) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(8) != null ? rs.getString(8) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(9) != null ? rs.getString(9) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(10) != null ? rs.getString(10) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(11) != null ? rs.getString(11) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(12) != null ? rs.getString(12) : "") + "\"");
                fw.append(',');
                fw.append("\"" + (rs.getString(13) != null ? rs.getString(13) : "") + "\"");
                fw.append('\n');
            }
            fw.flush();
            fw.close();
            conn.close();

            // send email
            log.info("send email......");
            sendEmailWithAttachment(toEmail, "Dollar General HVAC Daily Report", "Please use this report which is exported from Oracle in case you don't receive DG HVAC report from DOMO!", CSV_FILE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendEmailWithAttachment(String to, String subject, String body, String filename) throws MessagingException {

        Properties props = new Properties();
        props.put("mail.smtp.host", smtp_server);

        Session session = Session.getDefaultInstance(props, null);

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(fromEmail));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
        message.setRecipients(Message.RecipientType.CC, InternetAddress.parse("garys.sun@copeland.com"));
        message.setSubject(subject);

        MimeBodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setText(body);

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(messageBodyPart);

        messageBodyPart = new MimeBodyPart();
        DataSource source = new FileDataSource(filename);
        messageBodyPart.setDataHandler(new DataHandler(source));
        messageBodyPart.setFileName("Dollar General HVAC Daily Report.csv");
        multipart.addBodyPart(messageBodyPart);

        message.setContent(multipart);

        Transport.send(message);
        log.info("send email successfully!");
    }
}
