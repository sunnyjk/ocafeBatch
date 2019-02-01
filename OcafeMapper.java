package ocafe;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OcafeMapper {

    private final static Logger LOG = Logger.getLogger(OcafeKKOBatch.class);

    private static Connection con = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;

    public static Connection getConnection(){
        String connectionUrl = "jdbc:sqlserver://10.185.64.50:64789;" + "databaseName=cjpos_db";

        // Declare the JDBC objects.
        Connection con = null;

        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        con = DriverManager.getConnection(connectionUrl, "sa", "cjsystems");

    }catch (Exception e ){
        LOG.error(e.toString());
    }

        return con;
}

    public static List<HashMap<String, Object>> getKakaoTemp(){
        con = getConnection();
        List<HashMap<String, Object>> pushList = new ArrayList<>();

        try{
            /*String sql= "SELECT SEQ, TEMPLATE_CD, MAIL_ID, DTB_SEQ, STATUS_CD  FROM CAFETERIA_KAKAO_AT_TEMP WITH(NOLOCK)\n" +
                        "WHERE STATUS_CD = 1\n" +
                        "ORDER BY DTB_SEQ";*/
            String sql = "SELECT MAX(SEQ) SEQ, COUNT(*) CNT, TEMPLATE_CD, MAIL_ID, DTB_SEQ, STATUS_CD  FROM CAFETERIA_KAKAO_AT_TEMP WITH(NOLOCK)\n" +
                    "WHERE STATUS_CD = 0\n" +
                    "GROUP BY TEMPLATE_CD, MAIL_ID, DTB_SEQ, STATUS_CD\n" +
                    "ORDER BY DTB_SEQ";
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while(rs.next()){
               int kkoSeq = rs.getInt("SEQ");
               String tempCd = rs.getString("TEMPLATE_CD");
               String mailId = rs.getString("MAIL_ID");
               int dtbSeq = rs.getInt("DTB_SEQ");
               int cnt = rs.getInt("CNT");

                HashMap<String, Object> outParams = new HashMap<>();
                outParams.put("kkoSeq", kkoSeq);
                outParams.put("tempCd", tempCd);
                outParams.put("mailId", mailId);
                outParams.put("dtbSeq", dtbSeq);
                outParams.put("cnt", cnt);

                pushList.add(outParams);
                LOG.info("KAKAO Push data: " + tempCd + ", " + mailId + ", " + dtbSeq + ", " + cnt);
            }

        } catch (Exception e){
            LOG.error(e.toString());
        }finally {
            dbClose();
        }
        return pushList;
    }

    public static String getMobile(HashMap<String, Object> params){
        con = getConnection();
        String mobile = "";


        String sql = "{call dbo.USP_GET_MOBILE(?, ?)}";
        try{
            CallableStatement cstmt = con.prepareCall(sql);
            cstmt.setString("MAIL_ID", (String) params.get("mailId"));
            cstmt.registerOutParameter("MOBILE", Types.VARCHAR);
            cstmt.execute();

            mobile = cstmt.getString("MOBILE");

            cstmt.close();

        } catch (Exception e){
            LOG.error(e.toString());
        } finally {
            dbClose();
        }

        return mobile;
    }

    public static int getNoshowCnt(HashMap<String, Object> params){
        con = getConnection();
        int noShowCnt = 0;

        try {
            String sql = "SELECT NOSHOW_CNT FROM CAFETERIA_KAKAO_ORDER_USER_INFO WITH(NOLOCK) WHERE MAIL_ID = ?";

            pstmt = con.prepareStatement(sql);
            pstmt.setString(1, (String) params.get("mailId"));
            rs = pstmt.executeQuery();

            while(rs.next()){
                noShowCnt = rs.getInt("NOSHOW_CNT");
            }

        } catch(Exception e){
            LOG.error(e.toString());
        } finally {
            dbClose();
        }

        return noShowCnt;
    }

    public static ArrayList<HashMap<String, Object>> getWaitingInfo(HashMap<String, Object> params){
        con = getConnection();

        ArrayList<HashMap<String, Object>> list = new ArrayList<>();

        try {
            /*String sql = "EXEC SP_OPENSYMMETRICKEY \n" +
                    "SELECT" +
                    "            ROW_NUM" +
                    "            , A.MAIL_ID" +
                    "            , EMP.EMP_NO" +
                    "            , A.EMP_NM" +
                    "            , DBO.FN_DECRYPTBYDATA(MOBILE_ENC) MOBILE" +
                    "            , A.MENU_CNT" +
                    "            , A.MENU_DISP_NM_1" +
                    "            , CASE WHEN A.MENU_DISP_NM_2 IS NULL THEN '' ELSE A.MENU_DISP_NM_2 END AS MENU_DISP_NM_2" +
                    "            , CASE WHEN A.MENU_DISP_NM_3 IS NULL THEN '' ELSE A.MENU_DISP_NM_3 END AS MENU_DISP_NM_3" +
                    "            , CASE WHEN A.MENU_DISP_NM_4 IS NULL THEN '' ELSE A.MENU_DISP_NM_4 END AS MENU_DISP_NM_4" +
                    "            , CASE WHEN A.MENU_DISP_NM_5 IS NULL THEN '' ELSE A.MENU_DISP_NM_5 END AS MENU_DISP_NM_5" +
                    "            , CASE WHEN A.MENU_DISP_NM_6 IS NULL THEN '' ELSE A.MENU_DISP_NM_6 END AS MENU_DISP_NM_6" +
                    "            , CASE WHEN A.MENU_DISP_NM_7 IS NULL THEN '' ELSE A.MENU_DISP_NM_7 END AS MENU_DISP_NM_7" +
                    "            , CASE WHEN A.MENU_DISP_NM_8 IS NULL THEN '' ELSE A.MENU_DISP_NM_8 END AS MENU_DISP_NM_8" +
                    "            , CASE WHEN A.MENU_DISP_NM_9 IS NULL THEN '' ELSE A.MENU_DISP_NM_9 END AS MENU_DISP_NM_9" +
                    "            , A.TRAY_NO" +
                    "        FROM" +
                    "            (" +
                    "                SELECT" +
                    "                    ROW_NUMBER() OVER(ORDER BY SEQ) AS ROW_NUM" +
                    "                        , *" +
                    "                FROM CAFETERIA_DTB WITH(NOLOCK)" +
                    "                WHERE SALE_DATE = CONVERT(CHAR(8), GETDATE(), 112)" +
                    "                    AND STATUS = 'ORD'" +
                    "            ) A" +
                    "        LEFT JOIN CAFETERIA_CUSTMST EMP WITH(NOLOCK)" +
                    "        ON A.MAIL_ID = EMP.MAIL_ID" +
                    "        WHERE ROW_NUM = 5 AND A.TAKEOUT_YN = 'N' AND A.ORDER_METHOD = 'M'";*/
            String sql = "EXEC SP_OPENSYMMETRICKEY;\n" +
                    "SELECT\n" +
                    "    ROW_NUM\n" +
                    "    , A.MAIL_ID\n" +
                    "    , EMP.EMP_NO\n" +
                    "    , A.EMP_NM\n" +
                    "    , DBO.FN_DECRYPTBYDATA(MOBILE_ENC) MOBILE\n" +
                    "    , A.MENU_CNT\n" +
                    "    , A.MENU_DISP_NM_1\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_2 IS NULL THEN '' ELSE A.MENU_DISP_NM_2 END AS MENU_DISP_NM_2\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_3 IS NULL THEN '' ELSE A.MENU_DISP_NM_3 END AS MENU_DISP_NM_3\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_4 IS NULL THEN '' ELSE A.MENU_DISP_NM_4 END AS MENU_DISP_NM_4\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_5 IS NULL THEN '' ELSE A.MENU_DISP_NM_5 END AS MENU_DISP_NM_5\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_6 IS NULL THEN '' ELSE A.MENU_DISP_NM_6 END AS MENU_DISP_NM_6\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_7 IS NULL THEN '' ELSE A.MENU_DISP_NM_7 END AS MENU_DISP_NM_7\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_8 IS NULL THEN '' ELSE A.MENU_DISP_NM_8 END AS MENU_DISP_NM_8\n" +
                    "    , CASE WHEN A.MENU_DISP_NM_9 IS NULL THEN '' ELSE A.MENU_DISP_NM_9 END AS MENU_DISP_NM_9\n" +
                    "    , A.TRAY_NO\n" +
                    "FROM\n" +
                    "    (\n" +
                    "        SELECT\n" +
                    "            ROW_NUMBER() OVER(ORDER BY SEQ) AS ROW_NUM\n" +
                    "                , *\n" +
                    "        FROM CAFETERIA_DTB WITH(NOLOCK)\n" +
                    "        WHERE SALE_DATE = CONVERT(CHAR(8), GETDATE(), 112)\n" +
                    "            AND STATUS = 'ORD'\n" +
                    "    ) A\n" +
                    "LEFT JOIN CAFETERIA_CUSTMST EMP WITH(NOLOCK)\n" +
                    "ON A.MAIL_ID = EMP.MAIL_ID\n" +
                    "WHERE ROW_NUM BETWEEN 6-? AND 5 \n" +
                    "AND A.TAKEOUT_YN = 'N' AND A.ORDER_METHOD = 'M'";

            pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, (int) params.get("cnt"));
            rs = pstmt.executeQuery();

            while(rs.next()){
                String menu = rs.getString("MENU_DISP_NM_1");
                LOG.info("오카페 5번째 대기순서: " + rs.getString("MAIL_ID"));

                HashMap<String, Object> outParams = new HashMap<>();

                outParams.put("mobile", rs.getString("MOBILE"));
                outParams.put("trayNo", rs.getInt("TRAY_NO"));
                outParams.put("mailId", rs.getString("MAIL_ID"));
                outParams.put("menu", menu);

                list.add(outParams);
            }

        } catch(Exception e){
            LOG.error(e.toString());
        } finally {
            dbClose();
        }

        return list;
    }

    public static HashMap<String, Object> checkWaitingInfo(){
        con = getConnection();
        HashMap<String, Object> outParams = new HashMap<>();

        String[] mailId = new String[3];
        String[] mobile = new String[3];
        String[] menu = new String[3];
        int[] trayNo = new int[3];

        try {
            String sql = "EXEC SP_OPENSYMMETRICKEY \n" +
                    "SELECT" +
                    "            ROW_NUM" +
                    "            , A.MAIL_ID" +
                    "            , EMP.EMP_NO" +
                    "            , A.EMP_NM" +
                    "            , DBO.FN_DECRYPTBYDATA(EMP.MOBILE_ENC) MOBILE" +
                    "            , A.MENU_CNT" +
                    "            , A.MENU_DISP_NM_1" +
                    "            , CASE WHEN A.MENU_DISP_NM_2 IS NULL THEN '' ELSE A.MENU_DISP_NM_2 END AS MENU_DISP_NM_2" +
                    "            , CASE WHEN A.MENU_DISP_NM_3 IS NULL THEN '' ELSE A.MENU_DISP_NM_3 END AS MENU_DISP_NM_3" +
                    "            , CASE WHEN A.MENU_DISP_NM_4 IS NULL THEN '' ELSE A.MENU_DISP_NM_4 END AS MENU_DISP_NM_4" +
                    "            , CASE WHEN A.MENU_DISP_NM_5 IS NULL THEN '' ELSE A.MENU_DISP_NM_5 END AS MENU_DISP_NM_5" +
                    "            , CASE WHEN A.MENU_DISP_NM_6 IS NULL THEN '' ELSE A.MENU_DISP_NM_6 END AS MENU_DISP_NM_6" +
                    "            , CASE WHEN A.MENU_DISP_NM_7 IS NULL THEN '' ELSE A.MENU_DISP_NM_7 END AS MENU_DISP_NM_7" +
                    "            , CASE WHEN A.MENU_DISP_NM_8 IS NULL THEN '' ELSE A.MENU_DISP_NM_8 END AS MENU_DISP_NM_8" +
                    "            , CASE WHEN A.MENU_DISP_NM_9 IS NULL THEN '' ELSE A.MENU_DISP_NM_9 END AS MENU_DISP_NM_9" +
                    "            , A.TRAY_NO" +
                    "        FROM" +
                    "            (" +
                    "                SELECT" +
                    "                    ROW_NUMBER() OVER(ORDER BY SEQ) AS ROW_NUM" +
                    "                        , *" +
                    "                FROM CAFETERIA_DTB WITH(NOLOCK)" +
                    "                WHERE SALE_DATE = CONVERT(CHAR(8), GETDATE(), 112)" +
                    "                    AND STATUS = 'ORD'" +
                    "            ) A" +
                    "        LEFT JOIN CAFETERIA_CUSTMST EMP WITH(NOLOCK)" +
                    "        ON A.MAIL_ID = EMP.MAIL_ID" +
                    "        WHERE ROW_NUM IN (3,4,5) AND A.TAKEOUT_YN = 'N' AND A.ORDER_METHOD = 'M'";

            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();

            int i=0;
            while(rs.next()){
                mailId[i] = rs.getString("MAIL_ID");
                mobile[i] = rs.getString("MOBILE");
                menu[i] = rs.getString("MENU_DISP_NM_1");
                trayNo[i] = rs.getInt("TRAY_NO");
               // String menu = rs.getString("MENU_DISP_NM_1");
                LOG.info("오카페 " + rs.getString("ROW_NUM")+ "번째 대기순서: " + rs.getString("MAIL_ID"));
                i++;
            }

        } catch(Exception e){
            LOG.error(e.toString());
        } finally {
            dbClose();
        }

        for(int i=0; i<mailId.length; i++){
            outParams.put("tempCd", "OCFT003");
            outParams.put("mailId", mailId[i]);
            outParams.put("mobile", mobile[i]);

            String msg = "텀블러를 가지고 늦지않게 10층 카페로 와주세요 :)\n\n" +
                    "· 주문 메뉴 : " + menu[i] + "\n" +
                    "· 대기순서 : 5번\n" +
                    "· 트레이번호 : " + trayNo[i] + "\n\n" +
                    "주문 취소를 원하시면, '취소'를 입력해주세요.";

            outParams.put("msg", msg);
            insertPushAt(outParams);
            //updateKakaoTemp(result, 0);
        }

        return outParams;
    }


    public static void insertPushAt(HashMap<String, Object> params){
        con = getConnection();

        String mobile = (String) params.get("mobile");
        String tempCd = (String) params.get("tempCd");
        String msg = (String) params.get("msg");
        String mailId = (String) params.get("mailId");

        System.out.println("insertPushAt: " + mailId + ", " + tempCd + ", " + params.get("kkoSeq") + ", " + msg);

        try {
            //String sql = "{call dbo.USP_INSERT_KKOMSG (?, ?, ?, ?)}";
            String sql = "{call dbo.USP_INSERT_KAKAO_MSG (?, ?, ?, ?, ?)}";
            CallableStatement cstmt = con.prepareCall(sql);
            cstmt.setString("TEMPLATE_CD", tempCd);
            cstmt.setString("MOBILE", mobile);
            cstmt.setString("MSG", msg);
            cstmt.setString("MAIL_ID", mailId);
            cstmt.setInt("KKO_SEQ", (int) params.get("kkoSeq"));
            cstmt.execute();

            cstmt.close();

            /*String checkSQL = "SELECT TOP 1 STATUS_CD FROM LOG_PROC WITH(NOLOCK) " +
                                "WHERE PROC_NM = 'USP_INSERT_KKOMSG' AND MSG = ? " +
                                "AND APPLY_DT >= CONVERT(VARCHAR(10), GETDATE(), 121) " +
                                "ORDER BY APPLY_DT DESC";
            pstmt = con.prepareStatement(checkSQL);
            pstmt.setString(1, tempCd + ", " + mailId);
            rs = pstmt.executeQuery();

            while (rs.next()){
                if("S".equals(rs.getString("STATUS_CD"))){
                    result = 1;
                }

            }*/

        } catch(Exception e){
            LOG.error(e.toString());
        } finally {
            dbClose();
        }
    }

    public static void updateKakaoTemp(int result, int kkoSeq){
        con = getConnection();

        try{
            String sql = "UPDATE CAFETERIA_KAKAO_AT_TEMP                      SET STATUS_CD = ? WHERE SEQ = ?";
            pstmt = con.prepareStatement(sql);
            pstmt.setInt(2, kkoSeq);

            if(result > 0){
                pstmt.setString(1, "2");
            } else {
                pstmt.setString(1, "3");
            }
            pstmt.executeUpdate();

        } catch (Exception e){
            LOG.error(e.toString());
        } finally {
            dbClose();
        }
    }

    public static void checkSTBList(int compTime){
        con = getConnection();

        try{
            String sql= "SELECT SEQ, TEMPLATE_CD, DTB_SEQ, REPLACE(CONVERT(CHAR(12), APPLY_DT, 24), ':', '') AS CALLTIME FROM CAFETERIA_KAKAO_AT_TEMP WITH(NOLOCK) WHERE STATUS_CD = 2 AND TEMPLATE_CD = 'OCFT004'";
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while(rs.next()){
                int kkoSeq = rs.getInt("SEQ");
                int dtbSeq = rs.getInt("DTB_SEQ");
                String callTime = rs.getString("CALLTIME").trim();

                if(compTime - Integer.parseInt(callTime) >= 600){
                    String deleteSql = "EXEC USP_DELETE_STB_ORDER ?, ?";
                    pstmt = con.prepareStatement(deleteSql);
                    pstmt.setInt(1, dtbSeq);
                    pstmt.setInt(2, kkoSeq);
                    pstmt.executeUpdate();
                }
            }

/*            String deleteAll = "DELETE FROM CAFETERIA_KAKAO_AT_TEMP WHERE STATUS_CD = 2 AND TEMPLATE_CD != 'OCFT004')";
            pstmt = con.prepareStatement(deleteAll);
            pstmt.executeUpdate();*/

        } catch (Exception e){
            LOG.error(e.toString());
        }finally {
            dbClose();
        }
    }

    public static void dbClose(){
        try {
            pstmt.close();
            rs.close();
            con.close();
        } catch (Exception e){
            LOG.error(e.toString());
        }
    }
}
