package ocafe;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;


public class OcafeKKOBatch {

    private final static Logger LOG = Logger.getLogger(OcafeKKOBatch.class);
    private static OcafeMapper mapper = new OcafeMapper();

    public static void main(String[] args) {
        LOG.info("This is OCAFE Batch :D");

        List<HashMap<String, Object>> pushList = mapper.getKakaoTemp();

        for(int i=0; i<pushList.size(); i++){
            HashMap<String, Object> pushParam = pushList.get(i);
            try {
                sendKakaoPush(pushParam);
            } catch (Exception e){
                LOG.error(e.toString());
            }
        }

        SimpleDateFormat currentTime = new SimpleDateFormat("HH:mm:ss");
        String compTime = currentTime.format(Calendar.getInstance().getTime()).replace(":", "");
        mapper.checkSTBList(Integer.parseInt(compTime));

        LOG.info("Batch Finished.");
    }

    public static void sendKakaoPush(HashMap<String, Object> params) throws Exception {

        LOG.info("KAKAO template code: " + params.get("tempCd"));

        int kkoSeq = (int) params.get("kkoSeq");
        String tempCd = (String) params.get("tempCd");
        String mailId = (String) params.get("mailId");
        String mobile = "";
        String msg = "";

        if(!"OCFT003".equals(tempCd)){
            mobile = mapper.getMobile(params);
        }

        // 알림톡 파라미터 설정
        params.put("mobile", mobile);
        params.put("tempCd", tempCd);

        switch (tempCd){
            case "OCFT001": // 음료제조완료 알림
                LOG.info("[sendKakaoPush] TEMPLATE_CD: " + tempCd + ", MAIL_ID: " + mailId);
                msg = "주문하신 음료가 준비되었어요. \n" +
                        "카페 픽업대에서 찾아가세요 :>";
                break;
            case "OCFT002": // 머그컵 반납요청 알림
                LOG.info("[sendKakaoPush] TEMPLATE_CD: " + tempCd + ", MAIL_ID: " + mailId);
                msg = "대여하신 머그컵을 아직 반납하지 않으셨군요.\n\n" +
                        "빨리 반납하셔서 바리스타분의 워라밸을 지켜주세요! :>\n\n" +
                        "· 오카페 운영시간 : 8:00~17:00";
                break;
            case "OCFT003": // 대기 5번째 알림 // "OCFT006"
                LOG.info("[sendKakaoPush] TEMPLATE_CD: " + tempCd + "");

                ArrayList<HashMap<String, Object>> list = mapper.getWaitingInfo(params);
                for(int i=0; i<list.size(); i++){
                    HashMap<String, Object> outParam = list.get(i);
                    params.put("mobile", outParam.get("mobile"));
                    params.put("mailId", outParam.get("mailId"));

                    /*msg = "텀블러를 가지고 늦지않게 10층 카페로 와주세요 :)\n\n" +
                            "· 주문 메뉴 : " + outParam.get("menu") + "\n" +
                            "· 대기순서 : 5번\n" +
                            "· 트레이번호 : " + outParam.get("trayNo") + "\n\n" +
                            "주문 취소를 원하시면, '취소'를 입력해주세요.";*/

                    msg = "개인컵을 가지고 늦지않게 10층 카페로 와주세요 :)\n" +
                            "\n" +
                            "· 주문 메뉴 : " + outParam.get("menu") + "\n" +
                            "· 대기순서 : 5번\n" +
                            "· 트레이번호 : "+ outParam.get("trayNo") + "\n" +
                            "\n" +
                            "주문 취소를 원하시면, '취소'를 입력해주세요.";

                    params.put("msg", msg);
                    mapper.insertPushAt(params);
                }

                break;
            case "OCFT004": // 보류확정 알림 // "OCFT005"
                LOG.info("[sendKakaoPush] TEMPLATE_CD: " + tempCd + ", MAIL_ID: " + mailId);
                //params.put("tempCd", "OCFT005");
                int noShowCnt = mapper.getNoshowCnt(params);
                msg = "개인컵을 아직 안주셔서 음료를 만들 수 없어요ㅠㅠ\n" +
                        "지금부터 5분 뒤에는 주문이 자동 삭제되며, 노쇼로 분류될거예요. Hurry up!\n\n" +
                        "· 노쇼 횟수 : " + noShowCnt + " 회\n" +
                        "(5회 이상 시 온라인 주문 1개월 제한)\n" +
                        "\n" +
                        "주문 취소를 원하시면, '주문 취소'라고 입력하세요. ";
                break;
            default:
                LOG.info("[sendKakaoPush] TEMPLATE_CD: no data");
                return;
        }

        if(tempCd != "OCFT003"){
            params.put("msg", msg);
            mapper.insertPushAt(params);
        }

    }

    public static void checkWaitingKakaoPush() throws Exception {

        HashMap<String, Object> params = new HashMap<>();

        LOG.info("checkWaitingKakaoPush");
        params.put("tempCd", "OCFT003");

        /*HashMap<String, Object> outParam = mapper.getWaitingInfo(params);
        params.put("mobile", outParam.get("mobile"));
        params.put("mailId", outParam.get("mailId"));

        String msg = "텀블러를 가지고 늦지않게 10층 카페로 와주세요 :)\n\n" +
                "· 주문 메뉴 : " + outParam.get("menu") + "\n" +
                "· 대기순서 : 5번\n" +
                "· 트레이번호 : " + outParam.get("trayNo") + "\n\n" +
                "주문 취소를 원하시면, '취소'를 입력해주세요.";
        params.put("msg", msg);
        mapper.insertPushAt(params);*/
        /*params.put("callMethod", "checkWaitingKakaoPush");
        params.put("params", outParam.get("mailId") + "/" + outParam.get("trayNo") );
        mapper.addLog(params);*/

    }

}
