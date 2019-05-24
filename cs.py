import re
import time
import datetime
def getRequest(self):
    sDate = datetime.date.strftime(self.dNow, '%Y%m%d')
    SendTime = datetime.date.strftime(self.dNow, '%Y%m%d%H%M%S%f')[:-3]
    sTime = datetime.date.strftime(self.dNow, '%H%M%S%f')[:-3]
    sSerial = datetime.date.strftime(self.dNow, '%Y%m%d%f') + '0'
    sCerID = "NFQS.y.1001"

    sTransCode = 'QSKGJDZGX'
    sCIS = '100190004964204'
    iBankCode = 102
    sAccNo = '1001794319300108055'

    url = "http://192.168.81.10:448/servlet/ICBCCMPAPIReqServlet"
    params = {'userID': sCerID, 'PackageID': sSerial, 'SendTime': SendTime}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    post = 'Version=0.0.1.0&TransCode={sTransCode}&BankCode={iBankCode}&GroupCIS={sCIS}\
     &ID={sCerID}&PackageID={sSerial}&Cert=&reqData='
    xml = """
     <?xml version="1.0" encoding = "GBK"?>
         <CMS>
             <eb>
                 <pub>
                     <TransCode>{sTransCode}</TransCode>
                     <CIS>{sCIS}</CIS>
                     <BankCode>{iBankCode}</BankCode>
                     <ID>{sCerID}</ID>
                     <TranDate>{sDate}</TranDate>
                     <TranTime>{sTime}</TranTime>
                     <fSeqno>{sSerial}</fSeqno>
                 </pub>
                 <in>
                     <AcctNo>{sAccNo}</AcctNo>
                     <Status>2</Status>
                     <NextTag></NextTag>
                 </in>
             </eb>
         </CMS>
     """
    content = (post + xml).format(sTransCode=sTransCode, sCIS=sCIS, iBankCode=iBankCode,
                                  sCerID=sCerID, sDate=sDate, sTime=sTime,
                                  sSerial=sSerial, sAccNo=sAccNo)
    req = requests.Request(method='post', url=url, params=params, headers=headers, data=content)
    return req


print(getRequest('self'))