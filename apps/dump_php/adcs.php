
$submitAction=@@action;
if($submitAction=='Add CUG' || $submitAction=='Add data plan ADCS' || $submitAction=='Add Supplementary product' || $submitAction=='Adjust SubAccount' || $submitAction=='Class change in ADCS' || $submitAction=='Delete Supplementary product' || $submitAction=='Main offering change')
{


	class TestSim
	{

	  private $developmentScope='PRODUCTION';
	  private $username='MIFEIGW_testsim';
	  private $password='ItTestSim@12345';


	  //private $authorizationBasic='Zm5VM0hBTkxUdm5LeE8zZHB5ZzNERmNjTUdVYTpoNnJuSDBYTDZzb0hUd2JORThmbDhncXBmOVFh';

	  private $authorizationBasic='RGoyMmFZOXRZcXVPN1Jram9NZmZvRmh0SDIwYTpHMnBLSWptemdWR0lZNGFmcE5RaDBpUHdXaWth';
	  private $tokenGenarateUrl='https://apigate.robi.com.bd/token';
	  private $cug_url='https://apigate.robi.com.bd/cbs/cbsAddCUGMemb/v1/cbsAddCUGMember';

	  private $Add_data_plan_ADCS_URL='https://apigate.robi.com.bd/adcsPackProvisioningNormal/v1/packProvisioningNormal';
	  private $DataPackStatusCheckUrl='https://apigate.robi.com.bd/adcsqueryPlanPurchase/v1/queryPlanPurchase?transactionId';

	  private $adjustBalanceURL='https://apigate.robi.com.bd/ocsadjustAccount/v1/adjustaccount';

	  private $CbsAddSupplementaryOffering='https://apigate.robi.com.bd/cbs/CbsAddSupplementaryOffering/v1/cbsAddSupplementaryOffering';
	  private $cbsDeleteSupplementaryOff='https://apigate.robi.com.bd/cbs/CbsDeleteSupplementaryOffering/v1/cbsDeleteSupplementaryOffering';

	  //private $changeMainProduct_url='https://apigate.robi.com.bd/cbs/CbsChangingSubscriberOffering/v1/cbsChangingSubscriberOffering';
	  //private $changeMainProduct_url='https://apigate.robi.com.bd/ocs/changeMainProduct/v1/changeMainProduct';

	  private $changeMainProduct_url='https://apigate.robi.com.bd/cbs/CbsChangingPrimaryOffering/v1/cbsChangingPrimaryOffering';

	  private $adcs_authorizationBasic='cHJvY2Vzc21ha2VyX2FwaTpQcmNzbWFrZXJAMTIzNA==';
	  private $subscriber_update_class_change='http://10.16.222.102:8080/spcm-rest-ws/pcc/spcm/subscribers/update';

	  private function curlRequestPost($url='',$headers='',$params='')
	  {


	      $ch = curl_init();
	      curl_setopt($ch, CURLOPT_URL, $url);
	      curl_setopt($ch, CURLOPT_POST, 1);
	      curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	      curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
	      curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
	      curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 60);
	      curl_setopt($ch, CURLOPT_TIMEOUT, 60);
	      $result = curl_exec($ch);





			  if($result === false)
			  {
				  return array('ResultHeader'=>array('ResultCode'=>999,'ResultDesc'=>curl_error($ch)));
				  curl_close($ch);
			  }
			  else
			  {
				  curl_close($ch);
				  $obj = json_decode($result,true);
				  return $obj;
			  }



	  }



	  public function genarateAccessToken()
	  {
	      $url = $this->tokenGenarateUrl;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Basic '.$this->authorizationBasic,
	      );
	      $params = array(
	          'grant_type'=>'password',
	          'username'=>$this->username,
	          'password'=>$this->password,
	          'scope' => $this->developmentScope
	      );
	      $obj = $this->curlRequestPost($url,$headers,$params);
	      if(isset($obj['access_token']))
	      {
	          $access_token=$obj['access_token'];
	      }
	      else
	      {
	          $access_token=0;
	      }
	      return $access_token; die();
	  }

	  public function addCUG($access_token=0,$PrimaryIdentity='1833120270',$cug_id='SGC600002980')
	  {
	      $url = $this->cug_url;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );
	      $params = array(
	          'Version'=>'1',
	          'SubGroupCode'=>$cug_id,
	          'Mode' => 'I',
	          'OperatorID' => '351',
	          'BusinessCode' => 'CreateSubscriber',
	          'BEID' => '101',
	          'SubClass' => '1',
	          'PrimaryIdentity' => $PrimaryIdentity
	      );

	      $result = $this->curlRequestPost($url,$headers,$params);
	      return $result;
	  }

	  public function addDataPackADCS($access_token=0,$PrimaryIdentity='8801833120270',$planName='test_recur-1')
	  {
	      $url = $this->Add_data_plan_ADCS_URL;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );


	      //echo $planName; die();

	      $params = array(
	          'MSISDN'=>$PrimaryIdentity,
	          'name'=>$planName
	      );

	      $result = $this->curlRequestPost($url,$headers,$params);
	      return $result;
	  }

	  private function adjustAmountAddFraction($amount=0,$balanceTypeCode=2000)
	  {
	      //if only 2000
	    $number_array=array(3001,3000,3105,2515,3104,3102,3101,3103,2513,2514,2505,2503,2502,2240,130959,2500,2504,2501,2000,3501,2550);
	    //in_array(needle, haystack)
	    if(in_array($balanceTypeCode, $number_array))
	    {
	        $returnAmount=10000*$amount;
	        return $returnAmount;
	    }
	    else
	    {
	        return $amount;
	    }

	  }

	  public function adjustBalance($access_token=0,$PrimaryIdentity='1833120270',$amount='0',$balanceTypeCode=2000)
	  {
	      $url = $this->adjustBalanceURL;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );
	      $params = array(
	          'SubscriberNo'=>$PrimaryIdentity,
	          'CommandId'=>'AdjustAccount',
	          'RequestType'=>'Event',
	          'AccountType'=>$balanceTypeCode,
	          'CurrAcctChgAmt'=>$this->adjustAmountAddFraction($amount,$balanceTypeCode),
	          'OperateType'=>2,
	          'LogID'=>time(),
	          'TransactionId'=>'IGW_Zee5_'.time(),
	          'SequenceId'=>'IGW_Zee5_'.time(),
	          'Version'=>1,
	          'SerialNo'=>'IGW_Zee5_'.time(),
	          'AdditionalInfo'=>'IGW_Zee5_anymessage'
	      );

	      $result = $this->curlRequestPost($url,$headers,$params);
	      return $result;
	  }


	  public function deductBalance($access_token=0,$PrimaryIdentity='1833120270',$amount='0',$balanceTypeCode=2000)
	  {
	      $url = $this->adjustBalanceURL;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );
	      $params = array(
	          'SubscriberNo'=>$PrimaryIdentity,
	          'CommandId'=>'AdjustAccount',
	          'RequestType'=>'Event',
	          'AccountType'=>$balanceTypeCode,
	          'CurrAcctChgAmt'=>$this->adjustAmountAddFraction($amount,$balanceTypeCode),
	          'OperateType'=>2,
	          'LogID'=>time(),
	          'TransactionId'=>'IGW_Zee5_'.time(),
	          'SequenceId'=>'IGW_Zee5_'.time(),
	          'Version'=>1,
	          'SerialNo'=>'IGW_Zee5_'.time(),
	          'AdditionalInfo'=>'IGW_Zee5_anymessage'
	      );

	      $result = $this->curlRequestPost($url,$headers,$params);
	      return $result;
	  }


	  public function addSupplimentaryProduct($access_token=0,$PrimaryIdentity='8801833120270',$OfferingID='642064')
	  {
	      $url = $this->CbsAddSupplementaryOffering;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );

	      $params = array(
	          'PrimaryIdentity'=>$PrimaryIdentity,
	          'Version'=>'1',
	          'MessageSeq'=>'MIFEIGW_testsim_'.time(),
	          'BusinessCode'=>'1',
	          'BEID'=>101,
	          'OperatorID'=>'353',
	          'OfferingID'=>$OfferingID,
	          'BundledFlag'=>'S',
	          'OfferingClass'=>'I',
	          'Status'=>2,
	          'Mode'=>'I'
			  //'ExpirationTime'=>'2037010100000',
	      );

	      $result = $this->curlRequestPost($url,$headers,$params);



	      return $result;
	  }


	  public function deleteSupplimentaryProduct($access_token=0,$PrimaryIdentity='8801833120270',$OfferingID='642064')
	  {
	      $url = $this->cbsDeleteSupplementaryOff;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );

	      $params = array(
	          'PrimaryIdentity'=>$PrimaryIdentity,
	          'Version'=>'1',
	          'MessageSeq'=>'MIFEIGW_testsim_'.time(),
	          'BusinessCode'=>'QueryPAInfo',
	          'BEID'=>101,
	          'OperatorID'=>'353',
	          'OfferingID'=>$OfferingID,
	          'OfferingClass'=>'I'
	      );

	      $result = $this->curlRequestPost($url,$headers,$params);
	      return $result;
	  }



	  public function changeMainProduct($access_token=0,$PrimaryIdentity='1833120270',$Main_Offering_ID=358)
	  {
	      $url = $this->changeMainProduct_url;
	      $headers = array(
	          'Content-type: application/x-www-form-urlencoded',
	          'Authorization: Bearer '.$access_token,
	      );

	      /*
	      $params = array(
	          'Version'=>'1',
	          //'MessageSeq'=>$username.'_'.time(),
	          'BusinessCode'=>1,
	          'BEID'=>101,
	          'OperatorID'=>353,
	          'PrimaryIdentity' =>$PrimaryIdentity,
	          //'OldOfferingID' => 446,
	          //'NewOfferingID' => $Main_Offering_ID,
	          //'BundledFlag' =>"S",
	          //'Status' =>2,
	          //'Mode' =>"I",
	          //'AddOfferingID' =>$Main_Offering_ID,
	          'AddEffectiveMode' =>"I",
	          'AddExpirationTime' =>2037010100000,
			  'AddOfferingID'=>$Main_Offering_ID,
			  'AddActivationMode'=>'A'
	          //'DelOfferingID' =>900237
	      );*/

	      /*$params = array(
	          'CommandId'=>'ChangeMainProd',
	          'RequestType'=>'Event',
	          'SubscriberNo'=>$PrimaryIdentity,
	          'ValidMode'=>4050000,
	          'NewMainProductId'=>$Main_Offering_ID
	      );*/

	      $params = array(
	          'Version'=>1,
	          'MessageSeq'=>$username.'_'.time(),
	          'PrimaryIdentity' =>$PrimaryIdentity,
	          'BusinessCode'=>1,
	          'BEID'=>101,
	          'OperatorID'=>353,
	          'OldOfferingID' =>170,
	          'NewOfferingID' =>$Main_Offering_ID,
	          'Mode' =>"I"
	      );



	      $result = $this->curlRequestPost($url,$headers,$params);


	      return $result;
	  }

	  private function curlRequestPostReturnStatusCode($url='',$headers='',$strHtml='')
	  {

	      $ch = curl_init();
	        curl_setopt($ch, CURLOPT_URL, $url);
	        curl_setopt($ch, CURLOPT_POST, 1);
	        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
	        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
	        curl_setopt($ch, CURLOPT_POSTFIELDS,$strHtml);
	        $result = curl_exec($ch);
	        if($result === false)
	        {
	            return array('ResultHeader'=>array('ResultCode'=>999,'ResultDesc'=>curl_error($ch)));
	            curl_close($ch);
	        }
	        else
	        {
	            $httpCode = curl_getinfo($ch , CURLINFO_HTTP_CODE);
	            curl_close($ch);
	            if($httpCode==200)
	            {
	                return array('ResultHeader'=>array('ResultCode'=>0,'ResultDesc'=>'Operation Complete Successfully'));
	            }
	            else
	            {
	              return array('ResultHeader'=>array('ResultCode'=>999,'ResultDesc'=>$httpCode));
	            }
	        }


	  }

	  public function adcs_class_change($PrimaryIdentity='8801833122068',$testclass='TEST_Class')
	  {
	        $url = $this->subscriber_update_class_change;
	        $headers = array(
	            'Content-type: application/xml',
	            'Authorization: Basic '.$this->adcs_authorizationBasic,
	        );

	        $strHtml='<?xml version="1.0" encoding="UTF-8"?><spcm:subscriberProvisionList xmlns:spcm="http://www.tangotelecom.com/pcc/spcm/provision" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.tangotelecom.com/pcc/spcm/provisionSPCMProvision.xsd">
	  <spcm:length>1</spcm:length>
	  <spcm:updateSubscriber>
	  <spcm:msisdn>'.$PrimaryIdentity.'</spcm:msisdn>
	  <spcm:class>'.$testclass.'</spcm:class>
	  </spcm:updateSubscriber>
	  </spcm:subscriberProvisionList>';

	        $result = $this->curlRequestPostReturnStatusCode($url,$headers,$strHtml);

	        return $result;

	  }






	}



	function LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$msg,$status){

	  	$CREATED_AT=date('Y-m-d h:i:s');
			$CREATED_BY="";

	  		$insertSuccessString="INSERT INTO PMT_TESTSIM_API_TRANSACTION_HISTORY
					(MSISDN,MSISDN_TYPE,TRANSACTION_TYPE,API_RESPONSE_JSON,TRANSACTION_STATUS,TRANSACTION_AMOUNT,CREATED_AT,CREATED_BY,API_ATTEMPT)
					VALUES
					('$NUMBER','$rechargeTypeUP','$submitAction','$msg','$status','0','$CREATED_AT','$CREATED_BY','1')";
					executeQuery($insertSuccessString);

			if($status=="Failed"){
				$g = new G();
				$g->SendMessageText($msg, 'ERROR');

			}
			else
			{
				$g = new G();
				$g->SendMessageText($msg, 'SUCCESS');
				//PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
			}

	  }


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
		$withEightEigthMSisdn=$NUMBER;

		//echo $RechargeAmount;
		//die();
		$CREATED_AT=date('Y-m-d h:i:s');
			$CREATED_BY="";

		$msisdn=$rechargeNumber;



		$obj=new TestSim();
		$access_token=$obj->genarateAccessToken();
		if(!empty($access_token))
		{

		      //Action Service Start
//LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Failed, Due to regenarate access token, Try again now.','Failed');

  		  if($submitAction=='Class change in ADCS')
          {

                if(!empty(@@Class_Name))
                {
                    $Class_Name=@@Class_Name;

                    //'1833120270'
                    $queryObj=$obj->adcs_class_change($withEightEigthMSisdn,$Class_Name);
                    if($queryObj['ResultHeader']['ResultCode']==0)
                    {
                    	LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Success');
                    }
                    else
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Failed');
                    }
                }
                else
                {

                	LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Class Name Missing.','Failed');
                }

          }
          elseif($submitAction=='Add CUG')
          {

                if(!empty(@@CUG_ID))
                {
                    //echo $numLength; die();
                    $CUG_ID=@@CUG_ID;
                    //'1833120270'
                    $queryObj=$obj->addCUG($access_token,$msisdn,$CUG_ID);
                    if($queryObj['ResultHeader']['ResultCode']==0)
                    {
                    	LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Success');
                    }
                    else
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Failed');
                    }
                }
                else
                {
                	LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'CUG ID should not be empty','Failed');
                }

          }
          elseif($submitAction=='Main offering change')
          {

                if(!empty(@@Main_Offering_ID))
                {
                    $OfferingID=@@Main_Offering_ID;
                    $queryObj=$obj->changeMainProduct($access_token,$msisdn,$OfferingID);



                    if(isset($queryObj))
                    {
                        if(!empty($queryObj))
                        {
                            if($queryObj['ResultHeader']['ResultCode']==0 || $queryObj['ResultHeader']['ResultDesc']=="Operation successfully." || $queryObj['ResultHeader']['ResultDesc']=="Operation successfully" || $queryObj['ResultHeader']['ResultDesc']=="Operation successful.")
                            {
                                LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Success');
                            }
                            else
                            {
                                LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Failed');
                            }
                        }
                        else
                        {
                            LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'API not Responding','Failed');
                        }
                    }
                    else
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'API not Responding','Failed');
                    }
                }
                else
                {
                    LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Main Offering id required.','Failed');
                }

          }
          elseif($submitAction=='Add Supplementary product')
          {

                if(!empty(@@CBS_Product_ID))
                {
                    //echo $numLength; die();
                    $OfferingID=@@CBS_Product_ID;
                    //='8801833120270'

                    $queryObj=$obj->addSupplimentaryProduct($access_token,$msisdn,$OfferingID);

					if(empty($queryObj))
					{
						LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,"API Getting EMPTY RESPONSE",'Failed');
					}
					else
					{
						if($queryObj['ResultHeader']['ResultCode']==0)
						{
							LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Success');
						}
						else
						{
							LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Failed');
						}
					}
                }
                else
                {
                	 LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'CBS product id required.','Failed');
                }

          }
          elseif($submitAction=='Delete Supplementary product')
          {

                if(!empty(@@CBS_Product_ID))
                {
                    //echo $numLength; die();
                    $OfferingID=@@CBS_Product_ID;
                    //='8801833120270'

                    $queryObj=$obj->deleteSupplimentaryProduct($access_token,$msisdn,$OfferingID);
                    if($queryObj['ResultHeader']['ResultCode']==0)
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Success');
                    }
                    else
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Failed');
                    }
                }
                else
                {
                    LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'CBS product id required.','Failed');
                }

          }
          elseif($submitAction=='Adjust SubAccount')
          {

                if(!empty(@@Balance_Type_Code) && !empty(@@Amount))
                {

                    $Balance_Type_Code=@@Balance_Type_Code;
                    $Amount=@@Amount;
                    $recType=substr($Amount,0,1);
                    $number_array=array(3001,3000,3105,2515,3104,3102,3101,3103,2513,2514,2505,2503,2502,2240,130959,2500,2504,2501,2000,3501,2550);
                    if(in_array($Balance_Type_Code, $number_array))
                    {
                          if($Amount>1000)
                          {
                              LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Max Limit 1000.','Failed');
                          }
                          elseif(substr($Amount,1)>1000)
                          {
                              LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Max Limit 1000.','Failed');
                          }
                    }


                    if($recType=='-')
                    {
                        $queryObj=$obj->deductBalance($access_token,$msisdn,$Amount,$Balance_Type_Code);
                    }
                    else
                    {
                        $queryObj=$obj->deductBalance($access_token,$msisdn,$Amount,$Balance_Type_Code);
                    }

                    if($queryObj['ResultHeader']['ResultCode']==0 || $queryObj['ResultHeader']['ResultCode']==405000000)
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Success');
                    }
                    else
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResultHeader']['ResultDesc'],'Failed');
                    }
                }
                else
                {
                    //!empty($_POST['msisdn']) && !empty($_POST['Balance_Type_Code']) && !empty($_POST['Amount'])
                    if(empty(@@Balance_Type_Code))
                    {
                        LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Balance Type Code Should not be empty.','Failed');
                    }
                    elseif(empty(@@Amount))
                    {
                    	LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Adjust Amount Should not be empty','Failed');
                    }

                }

          }
          elseif($submitAction=='Add data plan ADCS')
          {



                    $plan_name=@@Data_Plan_Name;
                    if(!empty($plan_name))
                    {
                        $queryObj=$obj->addDataPackADCS($access_token,$withEightEigthMSisdn,$plan_name);

                        if(isset($queryObj['ResponseCode']))
                        {


//8801833120270

                          //test_recur-1
                          if($queryObj['ResponseCode']==0)
                          {
                              LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResponseMsg'],'Success');
                          }
                          else
                          {
                              LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResponseMsg'],'Failed');
                          }
                        }
                        else
                        {

                            LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,$queryObj['ResponseMsg'],'Failed');

                        }


                    }
                    else
                    {
                    	LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Failed, ADCS Data Plan Name Should Not Be Empty.','Failed');


                    }

                    //'1833120270'



          }

		      //Action Service end
          		PMFRedirectToStep(@@APPLICATION, @%INDEX, 'DYNAFORM','8076163285ce273dcaf3928076452425');
		}
		else
		{

			LogPushTab($NUMBER,$rechargeTypeUP,$submitAction,'Failed, Due to regenarate access token, Try again now.','Failed');
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

	