
$submitAction=@@action;
if($submitAction=='Subscriber refresh Prepaid')
{



	class CustomerDataQuery{

	  private $tokenGenarateUrl='https://apigate.robi.com.bd/token';
	  private $postpaidRechargeUrl='https://apigate.robi.com.bd/cbsQueryCustomerInfo/v1/cbsQueryCustomerInfo';

	  public function genarateAccessToken()
	  {
	      $url = $this->tokenGenarateUrl;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Basic RGoyMmFZOXRZcXVPN1Jram9NZmZvRmh0SDIwYTpHMnBLSWptemdWR0lZNGFmcE5RaDBpUHdXaWth',
	      );


	      $params = array(
	          'grant_type'=>'password',
	          'username'=>'MIFEIGW_testsim',
	          'password'=>'ItTestSim@12345',
	          'scope' => 'PRODUCTION'
	      );
	      $ch = curl_init();
	      curl_setopt($ch, CURLOPT_URL, $url);
	      curl_setopt($ch, CURLOPT_POST, 1);
	      curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	      curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
	      curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
	      curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 60);
	      curl_setopt($ch, CURLOPT_TIMEOUT, 60);

	      // This should be the default Content-type for POST requests
	      //curl_setopt($ch, CURLOPT_HTTPHEADER, array("Content-type: application/x-www-form-urlencoded"));

	      $result = curl_exec($ch);
	      if(curl_errno($ch) !== 0) {
	        curl_close($ch);
	        return 0; die();
	          //error_log('cURL error when connecting to ' . $url . ': ' . curl_error($ch));

	      }

	      curl_close($ch);
	      $obj = json_decode($result,true);



	      $access_token=$obj['access_token'];
	      return $access_token; die();
	  }

	  public function postpaidCustomerQuery($access_token=0,$PrimaryIdentity='1833120270')
	  {
	      $url = $this->postpaidRechargeUrl;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );
	      $params = array(
	          'BusinessCode'=>'QueryCustomerInfo',
	          'BEID'=>'101',
	          'PrimaryIdentity' => $PrimaryIdentity,
	          'OperatorID' => '353',
	          'MessageSeq' => time(),
	          'Version' => '1'
	      );

	      $ch = curl_init();
	      curl_setopt($ch, CURLOPT_URL, $url);
	      curl_setopt($ch, CURLOPT_POST, 1);
	      curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	      curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
	      curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
	      curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 60);
	      curl_setopt($ch, CURLOPT_TIMEOUT, 60);

	      // This should be the default Content-type for POST requests
	      //curl_setopt($ch, CURLOPT_HTTPHEADER, array("Content-type: application/x-www-form-urlencoded"));

	      $result = curl_exec($ch);
	      if(curl_errno($ch) !== 0) {
	        curl_close($ch);
	        return 0; die();
	          //error_log('cURL error when connecting to ' . $url . ': ' . curl_error($ch));

	      }

	      curl_close($ch);
	      $obj = json_decode($result,true);
	      //print_r($obj); die();
	      return $obj;
	  }





	}


	  $offeringID=0;
	  $obj=new CustomerDataQuery();
	  $access_token=$obj->genarateAccessToken();
	  if(!empty($access_token))
	  {

	  	  $NUMBER = @@MSISDN;
		  @@msisdn=$NUMBER;
		  $msisdn=$NUMBER;


		$result = executeQuery("SELECT * FROM PMT_DV_TESTSIMNUMBER WHERE TYPE!='POSTPAID' AND NUMBER ='+".$NUMBER."' LIMIT 1");
		if (is_array($result) and count($result) > 0)
		{

			$rechargeNumber=str_replace("+880","",$result[1]['NUMBER']);
			$rechargeType=strtolower($result[1]['TYPE']); //prepaid
			$rechargeTypeUP=strtoupper($result[1]['TYPE']); //prepaid
			$RechargeAmount=@@RechargeAmount;
			$RechargeAmountWithFraction=($RechargeAmount*10000);

			//echo $RechargeAmount;
			//die();
			$CREATED_AT=date('Y-m-d h:i:s');
				$CREATED_BY="SYSTEM";

			$msisdn=$rechargeNumber;

			if(strlen($msisdn)==10)
			{
			  		$queryObj=$obj->postpaidCustomerQuery($access_token,$rechargeNumber);
		      		$offeringID=$queryObj['QueryCustomerInfoResult']['Subscriber']['PrimaryOffering']['OfferingKey']['OfferingID'];

			}else{

			  	$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
					(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
					VALUES
					('$NUMBER','$rechargeTypeUP','$submitAction','Invalid MSISDN','Failed','0','$CREATED_AT','$CREATED_BY','1')";
					executeQuery($insertSuccessString);

			}

		}
		else
		{
			$g = new G();
			$g->SendMessageText("Refresh Failed, Invalid Test SIM MSISDN.", 'ERROR');
			PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
		}






	  }
	  else
	  {
	  	$g = new G();
		$g->SendMessageText("Refresh Failed, Invalid Test SIM MSISDN.", 'ERROR');
		PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
	  }


	  //die();



	class postpaidRefreshSubscriber
	{
		public function deleteSubscriver($number='1833120270')
		{
			$url ='http://10.16.30.203:4384/services/CBSInterfaceBusinessMgrService';
			$randomID=time();
			$userName="VAS_UAT_PROCESSMAKER";
			$password="RObi!@123";
			$operatorID=845;
			$strHtml='
			<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:bus="http://www.huawei.com/bme/cbsinterface/cbs/businessmgrmsg" xmlns:com="http://www.huawei.com/bme/cbsinterface/common" xmlns:bus1="http://www.huawei.com/bme/cbsinterface/cbs/businessmgr">
			   <soapenv:Body>
			      <bus:DeleteSubscriberRequestMsg>
			         <RequestHeader>
			            <com:CommandId>DeleteSubscriber</com:CommandId>
			            <com:Version>1</com:Version>
			            <com:TransactionId>'.$randomID.'</com:TransactionId>
			            <com:SequenceId>'.$randomID.'</com:SequenceId>
			            <com:RequestType>Event</com:RequestType>
			            <com:SessionEntity>
			               <com:Name>'.$userName.'</com:Name>
			               <com:Password>'.$password.'</com:Password>
			               <com:RemoteAddress>1</com:RemoteAddress>
			            </com:SessionEntity>
			            <com:OperatorID>'.$operatorID.'</com:OperatorID>
			            <com:SerialNo>'.$randomID.'</com:SerialNo>
			         </RequestHeader>
			         <DeleteSubscriberRequest>
			            <bus1:SubscriberNo>
			               <bus1:SubscriberNo>'.$number.'</bus1:SubscriberNo>
			            </bus1:SubscriberNo>
			         </DeleteSubscriberRequest>
			      </bus:DeleteSubscriberRequestMsg>
			   </soapenv:Body>
			</soapenv:Envelope>';
			//echo $strHtml; die();
			   $ch = curl_init();
			   curl_setopt( $ch, CURLOPT_URL, $url );
			   curl_setopt( $ch, CURLOPT_POST, true );
			   curl_setopt( $ch, CURLOPT_HTTPHEADER, array('Content-Type: text/xml'));
			   curl_setopt( $ch, CURLOPT_RETURNTRANSFER, true );
			   curl_setopt( $ch, CURLOPT_POSTFIELDS, $strHtml);
			   $result = curl_exec($ch);
			   curl_close($ch);


			    $doc = new DOMDocument();
				$doc->loadXML($result);

			   //$obj=json_decode($doc,true);


			$dd = $doc->getElementsByTagName('ResultDesc')->item(0)->nodeValue;
			if($dd=="Operation successful.")
			{
			   return 1;
			}
			else
			{
			   return 0;
			}
		}


		public function createSubscriber($number='1833120270',$offeringID=0)
		{
			$url ='http://10.16.30.203:4384/services/BcServices';
			$randomID=time();

			$userName="VAS_UAT_PROCESSMAKER";

			$password="RObi!@123";
			$operatorID=351;
			$strHtml='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:bcs="http://www.huawei.com/bme/cbsinterface/bcservices" xmlns:cbs="http://www.huawei.com/bme/cbsinterface/cbscommon" xmlns:bcc="http://www.huawei.com/bme/cbsinterface/bccommon">
			   <soapenv:Header/>
			   <soapenv:Body>
			      <bcs:CreateSubscriberRequestMsg>
			         <RequestHeader>
			            <cbs:Version>1</cbs:Version>
			            <cbs:BusinessCode>CreateSubscriber</cbs:BusinessCode>
			            <cbs:MessageSeq>'.$randomID.'</cbs:MessageSeq>
			            <cbs:OwnershipInfo>
			               <cbs:BEID>101</cbs:BEID>
			            </cbs:OwnershipInfo>
			            <cbs:AccessSecurity>
			               <cbs:LoginSystemCode>'.$userName.'</cbs:LoginSystemCode>
			               <cbs:Password>'.$password.'</cbs:Password>
			            </cbs:AccessSecurity>
			            <cbs:OperatorInfo>
			               <cbs:OperatorID>351</cbs:OperatorID>
			            </cbs:OperatorInfo>
			         </RequestHeader>
			         <CreateSubscriberRequest>
			            <bcs:RegisterCustomer OpType="1">
			               <bcs:CustKey>111'.$randomID.'</bcs:CustKey>
			               <bcs:CustInfo>
			                  <bcc:CustType>1</bcc:CustType>
			                  <bcc:CustNodeType>1</bcc:CustNodeType>
			                  <bcc:CustClass>1</bcc:CustClass>
			                  <!--Optional:-->
			                  <bcc:CustCode>custcode'.$randomID.'</bcc:CustCode>
			               </bcs:CustInfo>
			               <bcs:IndividualInfo>
			                  <!--Optional:-->
			                  <bcc:Gender>1</bcc:Gender>
			               </bcs:IndividualInfo>
			            </bcs:RegisterCustomer>
			            <!--0 to 2 repetitions:-->
			            <bcs:Account>
			               <bcs:AcctKey>acctkey'.$randomID.'</bcs:AcctKey>
			               <bcs:AcctInfo>
			                  <bcc:AcctCode>acctcode'.$randomID.'</bcc:AcctCode>
			                  <!--<bcc:BillCycleType>1</bcc:BillCycleType>-->
			                  <bcc:AcctType>1</bcc:AcctType>
			                  <bcc:PaymentType>0</bcc:PaymentType>
			                  <bcc:AcctClass>1</bcc:AcctClass>
			                  <bcc:CurrencyID>1012</bcc:CurrencyID>
			                  <!--Optional:-->
			                  <bcc:InitBalance>0</bcc:InitBalance>
			                  <bcc:AcctPayMethod>1</bcc:AcctPayMethod>
			               </bcs:AcctInfo>
			            </bcs:Account>
			            <bcs:Subscriber>
			               <bcs:SubscriberKey>subkey'.$randomID.'</bcs:SubscriberKey>
			               <bcs:SubscriberInfo>
			                  <bcc:SubBasicInfo/>
			                  <!--1 or more repetitions:1110094941-->
			                  <bcc:SubIdentity>
			                     <bcc:SubIdentityType>1</bcc:SubIdentityType>
			                     <bcc:SubIdentity>'.$number.'</bcc:SubIdentity>
			                     <bcc:PrimaryFlag>1</bcc:PrimaryFlag>
			                  </bcc:SubIdentity>
			                  <bcc:SubClass>1</bcc:SubClass>
			                  <bcc:NetworkType>1</bcc:NetworkType>
			                  <bcc:Status>1</bcc:Status>
			               </bcs:SubscriberInfo>
			               <bcs:SubPaymentMode>
			                  <bcs:PaymentMode>0</bcs:PaymentMode>
			                  <bcs:AcctKey>acctkey'.$randomID.'</bcs:AcctKey>
			               </bcs:SubPaymentMode>
			            </bcs:Subscriber>
			            <bcs:PrimaryOffering>
			               <bcc:OfferingKey>
			                  <bcc:OfferingID>'.$offeringID.'</bcc:OfferingID>
			               </bcc:OfferingKey>
			               <bcc:BundledFlag>S</bcc:BundledFlag>
			               <bcc:OfferingClass>I</bcc:OfferingClass>
			               <bcc:Status>1</bcc:Status>
			            </bcs:PrimaryOffering>
			         </CreateSubscriberRequest>
			      </bcs:CreateSubscriberRequestMsg>
			   </soapenv:Body>
			</soapenv:Envelope>';


			   $ch = curl_init();
			   curl_setopt( $ch, CURLOPT_URL, $url );
			   curl_setopt( $ch, CURLOPT_POST, true );
			   curl_setopt( $ch, CURLOPT_HTTPHEADER, array('Content-Type: text/xml'));
			   curl_setopt( $ch, CURLOPT_RETURNTRANSFER, true );
			   curl_setopt( $ch, CURLOPT_POSTFIELDS, $strHtml);
			   $result = curl_exec($ch);
			   curl_close($ch);

			   $doc = new DOMDocument();
			   $doc->loadXML($result);
			   $dd = $doc->getElementsByTagName('ResultDesc')->item(0)->nodeValue;
			   if($dd=="Operation successfully.")
			   {
			      return 1;
			   }
			   else
			   {
			      return 0;
			   }
		}
	}

	$response_status=0;


	$NUMBER = @@MSISDN;
	@@msisdn=$NUMBER;
	$msisdn=$NUMBER;
	$result = executeQuery("SELECT * FROM PMT_DV_TESTSIMNUMBER WHERE TYPE!='POSTPAID' AND  NUMBER ='+".$NUMBER."' LIMIT 1");
	if (is_array($result) and count($result) > 0)
	{

		$rechargeNumber=str_replace("+880","",$result[1]['NUMBER']);
		$rechargeType=strtolower($result[1]['TYPE']); //prepaid
		$rechargeTypeUP=strtoupper($result[1]['TYPE']); //prepaid
		$RechargeAmount=@@RechargeAmount;
		$RechargeAmountWithFraction=($RechargeAmount*10000);

		//echo $RechargeAmount;
		//die();
		$CREATED_AT=date('Y-m-d h:i:s');
			$CREATED_BY="";

		$msisdn=$rechargeNumber;

		$successCount=0;
		if(empty($msisdn))
		{
				$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
				(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
				VALUES
				('$NUMBER','$rechargeTypeUP','$submitAction','Invalid MSISDN','Failed','0','$CREATED_AT','$CREATED_BY','1')";
				executeQuery($insertSuccessString);
		}
		else
		{
			$obj=new postpaidRefreshSubscriber();


			$deleteSubscriver=$obj->deleteSubscriver($msisdn);
			if($deleteSubscriver==1)
			{
				$createSubscriber=$obj->createSubscriber($msisdn,$offeringID);
				if($createSubscriber==1)
				{
					$successCount=1;
					$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
					(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
					VALUES
					('$NUMBER','$rechargeTypeUP','$submitAction','Refresh Successful','Success','0','$CREATED_AT','$CREATED_BY','1')";
					executeQuery($insertSuccessString);


				}
				else
				{
					$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
					(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
					VALUES
					('$NUMBER','$rechargeTypeUP','$submitAction','Invalid MSISDN','Failed','0','$CREATED_AT','$CREATED_BY','1')";
					executeQuery($insertSuccessString);
				}
			}
			else
			{
				$createSubscriber=$obj->createSubscriber($msisdn);
				$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
				(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
				VALUES
				('$NUMBER','$rechargeTypeUP','$submitAction','Invalid MSISDN','Failed','0','$CREATED_AT','$CREATED_BY','1')";
				executeQuery($insertSuccessString);
			}
		}

		if($successCount==1){
			$g = new G();
			$g->SendMessageText("Subscriber Refresh successfully", 'SUCCESS');
			PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
		}
		else{
			$g = new G();
			$g->SendMessageText("Refresh Failed, PLEASE TRY AGAIN.", 'ERROR');
			PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
		}

	}
	else
	{
		$g = new G();
		$g->SendMessageText("Refresh Failed, Invalid Test SIM MSISDN.", 'ERROR');
		PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
	}
}