
$submitAction=@@action;
if($submitAction=='Recharge')
{



	class TestSim
	{

		private $tokenGenarateUrl='https://apigate.robi.com.bd/token';
		private $postpaidRechargeUrl='https://apigate.robi.com.bd/cbsPerformPayment/v1/performPayment';
		private $prepaidRechargeUrl='https://apigate.robi.com.bd/cbsprepaidRecharge/v1/prepaidRecharge';

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

		public function postpaidRecharge($access_token=0,$PrimaryIdentity='181********',$Amount=0)
		{
				$url = $this->postpaidRechargeUrl;
				$headers = array(
					'Content-type: application/x-www-form-urlencoded',
					'Authorization: Bearer '.$access_token,
				);
				$params = array(
					'BusinessCode'=>'ChangeConsumptionLimit',
					'BEID'=>'101',
					'BRID'=>'101',
					'OpType' => '2',
					'PrimaryIdentity' => $PrimaryIdentity,
					'PaymentType' => 'cash',
					'PayType' => '2',
					'PaymentMethod' => '1001',
					'Amount' => $Amount,
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
				return $obj; die();
		}

		public function prepaidRecharge($access_token=0,$PrimaryIdentity='181********',$Amount=0)
		{
				$url = $this->prepaidRechargeUrl;
				$headers = array(
					'Content-type: application/x-www-form-urlencoded',
					'Authorization: Bearer '.$access_token,
				);
				$params = array(
					'BusinessCode'=>'ChangeConsumptionLimit',
					'BEID'=>'101',
					'BRID'=>'101',
					'PrimaryIdentity' => $PrimaryIdentity,
					'PaymentType' => 'cash',
					'PaymentMethod' => '1001',
					'Amount' => $Amount,
					'Version' => '1',
					'CurrencyID' => '1012'
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
				return $obj; die();
		}



	}

	$response_status=0;
	$errorMsgFromAPI="Recharge Failed, Please try again.";

	$NUMBER = @@MSISDN;
	@@msisdn=$NUMBER;
	$msisdn=$NUMBER;
	$result = executeQuery("SELECT * FROM PMT_DV_TESTSIMNUMBER WHERE NUMBER ='+".$NUMBER."' LIMIT 1");
	if (is_array($result) and count($result) > 0)
	{

		$rechargeNumber=str_replace("+880","",$result[1]['NUMBER']);
		$rechargeType=strtolower($result[1]['TYPE']); //prepaid
		$rechargeTypeUP=strtoupper($result[1]['TYPE']); //prepaid
		$RechargeAmount=@@RechargeAmount;
		$RechargeAmountWithFraction=($RechargeAmount*10000);

		//echo $RechargeAmount;
		//die();




		$obj=new TestSim();
		$access_token=$obj->genarateAccessToken();

		//echo $access_token; die();

		if(!empty($access_token))
		{
			//echo $access_token; die(); //8dd1f61f-71e9-3e3f-829d-b7721a6c40c0

			if($rechargeType=="prepaid")
			{
				$result=$obj->prepaidRecharge($access_token,$rechargeNumber,$RechargeAmountWithFraction);
			}
			else
			{
				$result=$obj->postpaidRecharge($access_token,$rechargeNumber,$RechargeAmountWithFraction);
			}

			//echo "<pre>";

			//print_r($result); die();

			$api_response_log=serialize(json_encode($result));

			if(isset($result['ResultHeader']['ResultCode']))
			{
				if($result['ResultHeader']['ResultCode']==0)
				{
					$response_status=1;
				}
			}

			$errorMsgFromAPI="Recharge Failed, Please try again.";
			if(isset($result['ResultHeader']['ResultDesc']))
			{
				if($result['ResultHeader']['ResultDesc']==0)
				{
					$errorMsgFromAPI=$result['ResultHeader']['ResultDesc'];
				}
			}

			$CREATED_AT=date('Y-m-d h:i:s');
			$CREATED_BY="";
			//$CREATED_AT=date('Y-m-d h:i:s');

			//$insertStatus
			if($response_status==1)
			{
				$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
				(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
				VALUES
				('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Success','$RechargeAmount','$CREATED_AT','$CREATED_BY','1')";
				$insertStatus = executeQuery($insertSuccessString);
			}
			else
			{
				if(!isset($api_response_log))
				{
					$api_response_log=serialize(json_encode(array('msg' =>'Action Failed')));
				}

				$insertFailedString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
				(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
				VALUES
				('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Failed','$RechargeAmount','$CREATED_AT','$CREATED_BY','1')";
				$insertStatus = executeQuery($insertFailedString);
			}
		}
		else
		{
				//1st try failed start
				//2nd try start
				$access_token=$obj->genarateAccessToken();
				if(!empty($access_token))
				{
					if($rechargeType=="prepaid")
					{
						$result=$obj->prepaidRecharge($access_token,$rechargeNumber,$RechargeAmountWithFraction);
					}
					else
					{
						$result=$obj->postpaidRecharge($access_token,$rechargeNumber,$RechargeAmountWithFraction);
					}

					$api_response_log=serialize(json_encode($result));

					if(isset($result['ResultHeader']['ResultCode']))
					{
						if($result['ResultHeader']['ResultCode']==0)
						{
							$response_status=1;
						}
					}

					$errorMsgFromAPI="Recharge Failed, Please try again.";
					if(isset($result['ResultHeader']['ResultDesc']))
					{
						if($result['ResultHeader']['ResultDesc']==0)
						{
							$errorMsgFromAPI=$result['ResultHeader']['ResultDesc'];
						}
					}

					$CREATED_AT=date('Y-m-d h:i:s');
					$CREATED_BY="";
					//$CREATED_AT=date('Y-m-d h:i:s');

					//$insertStatus
					if($response_status==1)
					{
						$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
						(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
						VALUES
						('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Success','$RechargeAmount','$CREATED_AT','$CREATED_BY','2')";
						$insertStatus = executeQuery($insertSuccessString);
					}
					else
					{
						if(!isset($api_response_log))
						{
							$api_response_log=serialize(json_encode(array('msg' =>'Action Failed')));
						}

						$insertFailedString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
						(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
						VALUES
						('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Failed','$RechargeAmount','$CREATED_AT','$CREATED_BY','2')";
						$insertStatus = executeQuery($insertFailedString);
					}
				}
				else
				{
						//2nd try failed start
						//3rd try start
						$access_token=$obj->genarateAccessToken();
						if(!empty($access_token))
						{
							if($rechargeType=="prepaid")
							{
								$result=$obj->prepaidRecharge($access_token,$rechargeNumber,$RechargeAmountWithFraction);
							}
							else
							{
								$result=$obj->postpaidRecharge($access_token,$rechargeNumber,$RechargeAmountWithFraction);
							}

							$api_response_log=serialize(json_encode($result));

							if(isset($result['ResultHeader']['ResultCode']))
							{
								if($result['ResultHeader']['ResultCode']==0)
								{
									$response_status=1;
								}
							}

							$errorMsgFromAPI="Recharge Failed, Please try again.";
							if(isset($result['ResultHeader']['ResultDesc']))
							{
								if($result['ResultHeader']['ResultDesc']==0)
								{
									$errorMsgFromAPI=$result['ResultHeader']['ResultDesc'];
								}
							}

							$CREATED_AT=date('Y-m-d h:i:s');
							$CREATED_BY="";
							//$CREATED_AT=date('Y-m-d h:i:s');

							//$insertStatus
							if($response_status==1)
							{
								$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
								(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
								VALUES
								('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Success','$RechargeAmount','$CREATED_AT','$CREATED_BY','3')";
								$insertStatus = executeQuery($insertSuccessString);
							}
							else
							{
								if(!isset($api_response_log))
								{
									$api_response_log=serialize(json_encode(array('msg' =>'Action Failed')));
								}

								$insertFailedString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
								(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
								VALUES
								('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Failed','$RechargeAmount','$CREATED_AT','$CREATED_BY','3')";
								$insertStatus = executeQuery($insertFailedString);
							}
						}
						else
						{
								$api_response_log=serialize(json_encode(array('msg' =>'API access_token Not Accessable with 3rd attempt.')));
								$insertFailedString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
								(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
								VALUES
								('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Failed','$RechargeAmount','$CREATED_AT','$CREATED_BY','3')";
								$insertStatus = executeQuery($insertFailedString);
						}

						//2nd try failed end
				}

				//1st try failed end
		}





	}else{

		$rechargeNumber=str_replace("+880","",$result[1]['NUMBER']);
		$rechargeType=strtolower($result[1]['TYPE']); //prepaid
		$rechargeTypeUP=strtoupper($result[1]['TYPE']); //prepaid
		$RechargeAmount=@@RechargeAmount;

		$api_response_log=serialize(json_encode(array('msg' =>'Not a Testsim Number.')));
		$insertFailedString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
		(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
		VALUES
		('$NUMBER','$rechargeTypeUP','Recharge','$api_response_log','Failed','$RechargeAmount','$CREATED_AT','$CREATED_BY','0')";
		$insertStatus = executeQuery($insertFailedString);
	}

	$useridFROMReq=@@userid;

	$emailofRequester='';
	$result = executeQuery("select
	u.`USR_UID`, u.`USR_EMAIL`, hr.`EmployeeName`, hr.`EmployeeID`, hr.`Mobile`, hr.`Department`, hr.`Division`, hr.`Unit`, hr.`Designation`, hr.`HierarchyManagerName`, hr.`HierarchyManagerEmail` FROM wf_robiworkflow.`USERS` u INNER JOIN wf_robiworkflow.`RobiHRSupervisor` hr ON UPPER(u.`USR_EMAIL`) = UPPER(hr.`Email`) WHERE  hr.`EmployeeID` = '$useridFROMReq'");
	if (is_array($result) and count($result) > 0) {
	   $emailofRequester 	= $result[1]['USR_EMAIL'];
	}

	if(empty($emailofRequester))
	{
		$Remail="fahad@divergenttechbd.com";
	}


	//echo "Re Response"; die();

	if($response_status==1)
	{

		$subject = 'Test MSISDN '.$msisdn.' Balance Recharge Notification';
		//Request for Test MSISDN <MSISDN> Balance Adjustment

		$email = "naresh.chandra@robi.com.bd";
		$cc = strtolower("fahad@divergenttechbd.com");
		//$email = "fahad@divergenttechbd.com";

		PMFSendMessage(@@APPLICATION, 'Robi Workflow',$email, '','',$subject, 'Product Configuration Team mail.html');



		$g = new G();
		$g->SendMessageText("RECHARGE COMPLETED SUCCESSFULLY", 'SUCCESS');
		PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
	}
	else
	{


		//$errorMsgFromAPI="Recharge Failed, Please try again.";

		$g = new G();
		$g->SendMessageText($errorMsgFromAPI, 'ERROR');
		PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
	}

}