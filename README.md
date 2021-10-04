
<?php
/**
 * @package orderprocessing-ext
 * Ticket  - EASYDAY-3
 */
class OrderprocessingExt_Model_OrderShipment
{
    const BARCODESTATIC = 66;
    const INVOICEMAXLIMIT = 99999999;
    const INVOICEMINLIMIT = 00000001;

    private $logFileName = 'API/placeorder.log';
    /**
     * @desc: method to add record into sales_order_shipment table
     * @param type $data
     * @return boolean
     * @ticket: EASYDAY-3
     */
    protected $_notAllowedStatus = array(
        Orderprocessing_Service_Interface_StatusConstant::STATUS_NEW,
        Orderprocessing_Service_Interface_StatusConstant::STATUS_PAYMENT_PENDING,
        Orderprocessing_Service_Interface_StatusConstant::STATUS_EXPORTABLE,
        Orderprocessing_Service_Interface_StatusConstant::STATUS_EXPORTED);
    protected $_defaultItemStatus = Orderprocessing_Service_Interface_StatusConstant::STATUS_EXPORTED;
    private $responseHelper;

    public function __construct() {
        $this->responseHelper= new Helpers_ResponseHelper();
    }
    public function getAllowedStatus() {
        return $this->_notAllowedStatus;
    }
    public function addShipment($data, $mposCall = false){
        $shipmentTable = new DbTable_Sales_Order_ShipmentTable();
        $ins_shipment = array();
        $ins_shipment['fk_sales_order'] = $data['id_sales_order'];
        $ins_shipment['shipment_id'] = $data['order_nr']."_".$data['fk_sales_order_pickup_method'];
        $ins_shipment['fk_sales_order_pickup_method'] = $data['fk_sales_order_pickup_method'];
        $ins_shipment['order_nr'] = $data['order_nr'];
        $ins_shipment['fk_delivery_store_slot']=$data['fk_delivery_store_slot'];
        $ins_shipment['shipment_grandtotal'] = $data->getGrandTotal();
        if(!empty($data['payment_method']) && $data['payment_method'] == "CashOnDelivery"){
            $ins_shipment['status'] = 3;
        } else {
            $ins_shipment['status'] = 2;
        }
        $ins_shipment['created_at'] = date('Y-m-d h:i:s');
        if(isset($data['multiple_delivery_charge'])) {
            foreach($data['multiple_delivery_charge'] as $chargeDetail)
            {
                $ins_shipment['shipment_charge'] = $chargeDetail['deliveryCharge'];
                $ins_shipment['sku'] = $chargeDetail['sku'];
            }
        }
        try{
            $orderShipmentRow = $shipmentTable->findOneByCriteria(array(DbTable_Sales_Order_ShipmentRow::FK_SALES_ORDER => $data['id_sales_order']));
            if(!$orderShipmentRow){
                $rowId = $shipmentTable->insert($ins_shipment);
                if(!$mposCall) {
                    $awsSqsObj = new AwsSqs();
                    $queue_url = $awsSqsObj->getSqsQueUrlByName($awsSqsObj->_sqsShippmentQue);
                    if(!empty($queue_url)){
                        $messageParams = $awsSqsObj->getMessageParams(array($ins_shipment['shipment_id']), $queue_url, 10, "Shippment Ids", "PKD");
                        $awsSqsObj->sqsSendMessage($messageParams);
                    }
                }
            }else{
		$orderShipmentRow = $orderShipmentRow->toArray();
                $rowId = $orderShipmentRow['id_sales_order_shipment'];
            }

            if(!empty($rowId)){
                if($this->addShipmentItem($rowId, $data)){
                   return true;
                }
            }
        } catch (Exception $ex) {
            return false;
            echo "Exception in add shipment.".$ex->getMessage();
        }

    }
    /**
     * @task add multiple shipments to sales_order_shipment table API call
     * @author Jyoti Jakhmola <jyoti.jakhmola@embitel.com>
     * @param type $data
     * @return boolean
     */
    public function addMultipleShipment($data , $multipleShipmentArr, $calculatedMultipleDeliveryCharge){
        Bob_Log::debug(__FUNCTION__.'() multipleShipmentArr '. var_export($multipleShipmentArr,true), 'API/queue.log');
        Bob_Log::debug(__FUNCTION__.'() calculatedMultipleDeliveryCharge '. var_export($calculatedMultipleDeliveryCharge,true), 'API/queue.log');
        $orderNumber = $data['order_nr'];
        $salesOrderId = $data['id_sales_order'];
        $itemCollection = $data['item_collection'];
        $shipmentItem = [];
        $itemArray = [];
        foreach ($itemCollection as $item){
            $itemsku = $item->getSku();
            $itemArray[$itemsku]['paid_price'] = $item->getPaidPrice();
            $itemArray[$itemsku]['store_code'] = $item->getStoreCode();
        }
        try{
            if(count($multipleShipmentArr) > 0 && count($itemCollection) > 0  ) {
                $isCod = (!empty($data['payment_method']) && $data['payment_method'] == "CashOnDelivery") ? true:false;
                $isRoundUpNeeded = $this->_isRoundUp($data->getAddressShipping()->getFkCustomerAddressRegion());
                foreach($multipleShipmentArr as $key => $value) {
                    $shipmentStoreCode = '';
                    $value = (array) $value;
                    $deliverySlot = isset($value['deliverySlot']) ? (int) $value['deliverySlot'] : null ;// default
                    $skuList = isset($value['sku']) ? (array) $value['sku'] : [] ;// default
                    $priceTotal = 0;
                    foreach ($skuList as $va){
                        $priceTotal += $itemArray[$va]['paid_price'];
                        $shipmentStoreCode = empty($shipmentStoreCode) ? $itemArray[$va]['store_code'] : $shipmentStoreCode;
                    }
                    $shipmentTable = new DbTable_Sales_Order_ShipmentTable();
                    $ins_shipment = array();
                    $ins_shipment['fk_sales_order'] = $salesOrderId;
                    $ins_shipment['order_nr'] = $orderNumber;
                    $ins_shipment['shipment_id'] = $orderNumber."_".$key;
                    if(!empty($data['payment_method']) && $data['payment_method'] == "CashOnDelivery"){
                        $ins_shipment['status'] = 3;
                    } else {
                        $ins_shipment['status'] = 2;
                    }

                    $shipmentCharge = isset($calculatedMultipleDeliveryCharge[$key]['deliveryCharge'])
                                                        ? $calculatedMultipleDeliveryCharge[$key]['deliveryCharge'] : 0 ;// default;

                    $ins_shipment['shipment_charge'] = $shipmentCharge;
                    $ins_shipment['old_shipment_charge'] = $shipmentCharge;
                    /*
                    $ins_shipment['shipment_grandtotal'] = isset($calculatedMultipleDeliveryCharge[$key]['amount'])
                                                    ? $calculatedMultipleDeliveryCharge[$key]['amount'] : 0 ;// default;

                    */

                    //Rounding shipment grandtotal for COD orders grandtotal value
                    $utility = new ConfigurationExt_Service_Utility();
                    $roundValue = $utility->roundValue($priceTotal, $isRoundUpNeeded);
                    $shipmentItemsPriceTotal = ($isCod && isset($roundValue['round_value']))? $roundValue['round_value']: $priceTotal;
                    $ins_shipment['shipment_grandtotal'] = $shipmentItemsPriceTotal + $shipmentCharge;
                    $ins_shipment['sku'] = isset($calculatedMultipleDeliveryCharge[$key]['sku'])
                                                        ? $calculatedMultipleDeliveryCharge[$key]['sku'] : '' ;// default;
                    $ins_shipment['old_sku'] = isset($calculatedMultipleDeliveryCharge[$key]['sku'])
                                                        ? $calculatedMultipleDeliveryCharge[$key]['sku'] : '' ;

                    $ins_shipment['fk_sales_order_pickup_method'] = $key;// In Progress
                    $ins_shipment['fk_delivery_store_slot'] = $deliverySlot;
                    $ins_shipment['created_at'] = date('Y-m-d h:i:s');

                    $ins_shipment['store_format'] = ($key == '5' || $key == '6') ? 'BB' : 'ED';
                    $ins_shipment['store_code'] = $shipmentStoreCode;
                    Bob_Log::debug(__FUNCTION__.'() ins_shipment '. var_export($ins_shipment,true), 'API/queue.log');
                    $orderShipmentRow = $shipmentTable->findOneByCriteria(array(DbTable_Sales_Order_ShipmentRow::SHIPMENT_ID => $ins_shipment['shipment_id']));
                    Bob_Log::debug(__FUNCTION__.'() orderShipmentRow '. var_export($orderShipmentRow,true), 'API/queue.log');
                    if(!$orderShipmentRow){
                        $rowId = $shipmentTable->insert($ins_shipment);

                        //Add record to audit shipment table
                        $auditShipmentModel = new OrderprocessingExt_Model_AuditShipment();
                        $insertRecord = $auditShipmentModel->sendToAuditShipmentModification($ins_shipment['shipment_id'], $ins_shipment['shipment_grandtotal']);

//                        $awsSqsObj = new AwsSqs();
//                        $queue_url = $awsSqsObj->getSqsQueUrlByName($awsSqsObj->_sqsShippmentQue);
//                        Bob_Log::debug(' IF of orderShipmentRow :  queue_url '. var_export($queue_url,true), 'API/queue.log');
//                        if(!empty($queue_url)){
//                            $messageParams = $awsSqsObj->getMessageParams(array($ins_shipment['shipment_id']), $queue_url, 10, "Shippment Ids", "PKD");
//                            $awsSqsObj->sqsSendMessage($messageParams);
//                        }
                    } else {
//                        Bob_Log::debug(' ELSE of orderShipmentRow '. var_export($orderShipmentRow,true), 'API/queue.log');
                        $orderShipmentRow = $orderShipmentRow->toArray();
                        $rowId = $orderShipmentRow['id_sales_order_shipment'];
                    }
                    if(!empty($rowId)){
                        foreach($data['item_collection'] as $k => $val) {
                            if(in_array($val['sku'], $skuList)) {
    //                            $shipmentItem[$rowId][] = $val;
                                $this->addMultipleShipmentItem($rowId, $val);
                            }
                        }
                    }
                }
            } else {
                Bob_Log::debug('OrderShipment.php , MULTIPLESHIPMENT AND ITEMCOLLECTION COUNT 0', $this->logFileName);
                return false;
            }
        } catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug('OrderShipment.php , Exception in addMultipleShipment() - : '. var_export($exceptionMsg,true), $this->logFileName);
        }
    }
    /**
     * @desc: method to add record into sales_order_shipment_item table
     * @param type $shipmentId
     * @param type $data
     * @return int
     * @ticket: EASYDAY-3
     */
    public function addShipmentItem($shipmentId = null, $data = null){
        $count = 0;
        $shipmentItemTable = new DbTable_Sales_Order_Shipment_ItemTable();
        foreach($data['item_collection'] as $key => $item){
            $ins_shipment_item = array();
            $ins_shipment_item['fk_sales_order_shipment'] = $shipmentId;
            $ins_shipment_item['fk_sales_order_item'] = $item['id_sales_order_item'];
            $ins_shipment_item['status'] = 0;
            $ins_shipment_item['created_at'] = date('Y-m-d h:i:s');
            try{
                $orderShipmentItemRow = $shipmentItemTable->findOneByCriteria(array(DbTable_Sales_Order_Shipment_ItemRow::FK_SALES_ORDER_ITEM => $item['id_sales_order_item']));
                if(!$orderShipmentItemRow){
                    if($shipmentItemTable->insert($ins_shipment_item)){
                        $count++;
                    }
                }

            } catch (Exception $ex) {
                echo "Exception in add shipmentItem.".$ex->getMessage();
            }

        }
        return $count;
    }
    /**
     * @desc: method to add record into sales_order_shipment_item table for APP as per mutliple shipments
     * @task Jyoti Jakhmola <jyoti.jakhmola@embitel.com>
     * @param type $shipmentId
     * @param type $data
     * @return int
     * @ticket: PKD-1262
     */
    public function addMultipleShipmentItem($shipmentId = null, $item = null) {
        $count = 0;
        try{
            $shipmentItemTable = new DbTable_Sales_Order_Shipment_ItemTable();
                $ins_shipment_item = array();
                $ins_shipment_item['fk_sales_order_shipment'] = $shipmentId;
                $ins_shipment_item['fk_sales_order_item'] = $item['id_sales_order_item'];
                $ins_shipment_item['status'] = 0;
                $ins_shipment_item['created_at'] = date('Y-m-d h:i:s');
                $orderShipmentItemRow = $shipmentItemTable->findOneByCriteria(array(DbTable_Sales_Order_Shipment_ItemRow::FK_SALES_ORDER_ITEM => $item['id_sales_order_item']));
                    if($shipmentItemTable->insert($ins_shipment_item)){
                        $count++;
                }
        } catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug('OrderShipment.php , Exception in addMultipleShipmentItem() - : '. var_export($exceptionMsg,true), $this->logFileName);
        }
        return $count;
    }
    /**
     * @desc: method to get all items by shipmentId
     * @param type $shipmentId
     * @return type
     */
    public function getItemsByShipmentId($shipmentId = null){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('s' => 'sales_order_shipment'),
                    array('shipment_id','order_nr','status','fk_sales_order','fk_delivery_store_slot','fk_sales_order_pickup_method'))
                ->join(array('i'=>'sales_order_shipment_item'),'s.id_sales_order_shipment = i.fk_sales_order_shipment',
                        array('i.fk_sales_order_item'))
                ->where("s.shipment_id = '$shipmentId'");
        return $select->query()->fetchAll();
    }

    public function getShipmentIdfromInvoiceBarcode($invoice_barcode = null)
    {
        $db = Zend_Db_Table::getDefaultAdapter();

        $select = $db->select()
            ->from(array('s' => 'sales_invoice'),
                array('shipment_id'))
            ->where("s.invoice_barcode = '$invoice_barcode'");
        return $select->query()->fetchAll();
    }

    public function getLastShipmentId(){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('s' => 'sales_order_shipment'),
                    array('shipment_id','order_nr','status','fk_sales_order','fk_delivery_store_slot','fk_sales_order_pickup_method'))
                ->limit(1);
        return $select->query()->fetchAll();
    }

    /**
     * @ticket PKD-39
     * @param type $shipmentId
     * @param type $status
     * @return type
     */

    public function getOrderIdByShipmentIdAndStatus($shipmentId = null, $status = null ){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('s' => 'sales_order_shipment'),
                    array('shipment_id','order_nr','status','fk_sales_order'));
        if(!empty($shipmentId)){
            $select->where("s.shipment_id = '$shipmentId'");
        }
        if(!empty($status)){
            $select->where("s.status = '$status'");
        }
        return $select->query()->fetchAll();
    }


    public function getItemImge($data){
        $ci = new CatalogExt_Service_Image();
        $imageModel = $ci->getImageModel();
        $image = $imageModel->getImageUrlBySku($data);
        return $image;
    }
    /**
     * @desc: REST API CAll to get Shipment Order Details (order details, item details, shipment details, payment details)
     * @param string $shipment_id
     * @return boolean
     * @ticket: EASYDAY-3
     */


    public function shipmentDetails($shipment_id, $statusCode=null){

        $invoice_barcode_flag = false;
        $all_items = $this->getItemsByShipmentId($shipment_id);

        if (!empty($all_items)) {
            $shipment =array();
            foreach($all_items as $item){
                $all_shipment_items[] = $item['fk_sales_order_item'];
                $order_nr = $item['order_nr'];
                $shipment['shipment_id'] =  $item['shipment_id'];
                $shipment['status'] =  $item['status'];
                $shipment['fk_delivery_store_slot'] =  $item['fk_delivery_store_slot'];
                $shipment['fk_sales_order_pickup_method'] =  $item['fk_sales_order_pickup_method'];
            }

            if (!strpos($shipment_id, "_")) {
                $invoice_barcode_flag = true;
                $shipment_id = $this->getShipmentIdfromInvoiceBarcode($shipment_id);
                if (isset($shipment_id)) {
                    $shipment_id = $shipment_id[0]['shipment_id'];
                } else {
                    Bob_Log::debug('shipment_id not present in table ', 'API/queue.log');
                    return false;
                }
            }
        }

        if(!empty($order_nr)){
            $ordProcessExt = new OrderprocessingExt_Service_FindOrder();
            $order = $ordProcessExt->findOrderByOrderNr($order_nr,true);
            $rowData = $this->_createShipmentRowData($order, $shipment, $all_shipment_items, $statusCode, $invoice_barcode_flag);
            return $rowData;
        }

        return false;
    }
    /**
     * @desc this method is designed to create json structure as per given format
     * @param type $order_data
     * @param type $shipment
     * @param type $all_shipment_items
     * @return type
     * @ticket: EASYDAY-3
     */

    private function _createShipmentRowData($order, $shipment, $all_shipment_items, $statusCode=null, $invoice_barcode_flag){
        $ret_array = array();
        $shipping_address = $order->getAddressShipping();
        $billing_address = $order->getAddressBilling();
        $allItems = $order->getItemCollection()->toArray();
        $pickupMethod = $order->getPickupMethod();
        $order_data = $order->toArray();
        $shipmentDeliveryId = $order_data['fk_delivery_store_slot'];
        $cancelReason = new OrderprocessingExt_Model_Order();

        //PKD-4081 | PKD-4522 bug fix
        if (!empty($statusCode) && is_array($statusCode)) {
            $canceledStatus = array();
            $ConfigObj= new ConfigurationExt_Service_Loader_Namespace('orderprocessing-ext', true);
            $jsonStatus = $ConfigObj->getValue('order_modify_status');
            $jsonStatus = json_decode($jsonStatus,TRUE);

            if(!empty($jsonStatus) && !empty($jsonStatus['canceled']['status'])) {
                $canceledStatus = explode(",",$jsonStatus['canceled']['status']);
            }
        }

        /**
         * PKD-2078 changes
         */
        $shipment_details = []; //shipment response array
        $shipmentService = new OrderprocessingExt_Service_Shipment();

        // get local storage adapter to get categories and attrbiutes
        $localStoreObj = new Api_Service_Service();
        $localStorageAdapter = $localStoreObj->getStorageAdapter();

        // get store code
        $storeId = $order->getStoreId();
        //storeId should be fetch from sales_order_shipment & storeMaster table
        $result = $this->_getShipmentFormatStoreCode($shipment['shipment_id']);
        if(isset($result['id_store'])){
            $storeId = $result['id_store'];
        }
        $storeService = new Storemaster_Service_StoremasterAddress();
        $store = $storeService->getStoreByStoreId($storeId);
        $storeCode = (isset($store['store_code'])) ? $store['store_code'] : '';

        if (empty($shipmentDeliveryId) || $shipmentDeliveryId == '' || $shipmentDeliveryId == null){
            $shipmentDeliveryId = $shipment['fk_delivery_store_slot'];
            $pickupMethod['id_sales_order_pickup_method'] = $shipment['fk_sales_order_pickup_method'];
        }
        if(isset($shipmentDeliveryId) && !empty($shipmentDeliveryId)){
            $deliverySlot = new OrderprocessingExt_Service_Deliveryslot();
            $deliverySlotData = $deliverySlot->findDeliveryStoreSlotById($shipmentDeliveryId);
        }
        //$allowed_status = $this->getAllowedStatus();
        if(!empty($statusCode) && is_array($statusCode)){
            $allowed_status = $statusCode; //if statusCode is available use statuscode to filter items

        }else{
            $shipmentService = new OrderprocessingExt_Service_Shipment();
            $allowed_status = $shipmentService->getModel()->getStatesCodeByName(array('payment_pending','exportable','exported','clarify_not_shipped','ex_intransit','ex_shipped','ed_inward'));
        }

        $items = array();
        $itemSkuEan = array();
        $totalShipmentAmount = 0;
        $skuPrices = []; // PKD-2078

        foreach ($allItems as $value) {
//            $itemSkuEan[$value['sku']] = (empty($itemSkuEan[$value['sku']])) ? $this->getEanBySku($value['sku']) : $itemSkuEan[$value['sku']];
            $itemSkuEan[$value['sku']] = (empty($itemSkuEan[$value['sku']])) ? $this->getEanBySkutoPrice($value['sku'],$storeId) : $itemSkuEan[$value['sku']];
        }

        foreach($allItems as $id_soitem => $item){
           // $item_status = $this->getStatus($item['fk_sales_order_item_status']);
            if(in_array($item['fk_sales_order_item_status'], $allowed_status)){
                $item_details = array();
                if(in_array($id_soitem, $all_shipment_items)){
                    /**
                     * PKD-2078 additional fields in response
                     * get config attributes
                     */
                    $skuParts = explode('-', $item['sku']);
                    $configSku = (isset($skuParts[0])) ? $skuParts[0] : '';
                    $key = $configSku.'_'.$storeCode;
//                    $configAttributes = $localStorageAdapter->get('skustore', $key);
                    $configAttributes = $localStorageAdapter->get('product', $configSku);

                    // Assign the sku-to-store simple values into the appropriate keys in product data simples
//                    if (!empty($productData) && !empty($configAttributes)) {
//                        foreach ($productData['simples'] as $simpleSku => &$simpleData) {
//                            if (isset($configAttributes['simples'][$simpleSku])) {
//                                $skuStoreSimple = $configAttributes['simples'][$simpleSku];
//                                if (isset($skuStoreSimple['meta'])) {
//                                    $simpleData['meta']['price']            = $skuStoreSimple['meta']['price'];
//                                    $simpleData['meta']['special_price']    = $skuStoreSimple['meta']['special_price'];
//                                    $simpleData['meta']['member_price']     = $skuStoreSimple['meta']['member_price'];
//                                    $simpleData['meta']['saleablequantity'] = $skuStoreSimple['meta']['saleablequantity'];
//                                    $simpleData['meta']['quantity']         = $skuStoreSimple['meta']['quantity'];
//                                    $simpleData['meta']['threshold']        = $skuStoreSimple['meta']['threshold'];
//                                }
//                            }
//                        } // end: foreach simples
//                        $configAttributes = $productData;
//                    }

                    $item_details['orderItemLineId'] = $item['id_sales_order_item'];
                    $item_details['sku'] = $item['sku'];

                    // get brand name to add as a prefix for item name
                    $brandService = new Catalog_Service_Brand();
                    $brandName = $brandService->getBrandNameBySimpleSku($item['sku']);
                    $item_details['name'] = ($brandName) ? $brandName.' '.$item['name'] : $item['name'];

                    $item_details['qty'] = $item['quantity'];
                    // $item_details['image'] = $this->getItemImge(array('sku'=>$item['sku']));

                    // get one simple image
                    $item_details['image'] = '';
                    if( ! empty($configAttributes['simples'][$item['sku']]['images'][0]['url'])) {
                        $item_details['image'] = $configAttributes['simples'][$item['sku']]['images'][0]['url'] . '-catalog.jpg';
                    }
                    //if simple image is not present, get product image
                    if(isset($item_details['image']) && empty($item_details['image'])){
                        $item_details['image'] = isset($configAttributes['image']) ? strstr($configAttributes['image'], '.jpg') ? $configAttributes['image'] : $configAttributes['image']."-catalog.jpg" : '';
                     }
                    // get all the prices for Simple SKU - PKD-2078
                    if( ! array_key_exists($item['sku'], $skuPrices)) {
                        $skuToStoreService = new Skutostore_Service_Skutostore();
                        $prices = $skuToStoreService->getSimpleSkuStorePrices($item['sku'], $storeId);
                        if($prices) {
                            $skuPrices[$item['sku']] = $prices;
                        }
                    }

                    $item_details['MRP'] = []; // store prices
                    if(( ! empty($skuPrices[$item['sku']]))) {
                        $item_details['MRP'] = $skuPrices[$item['sku']];
                    }

//                    $item_details['EAN'] = (!empty($itemSkuEan[$item['sku']])) ? $itemSkuEan[$item['sku']] : $this->getEanBySku($item['sku']);
                    $item_details['EAN'] = (!empty($itemSkuEan[$item['sku']])) ? $itemSkuEan[$item['sku']] : $this->getEanBySkutoPrice($item['sku'],$storeId);
                    $item_details['UOM'] = '';
                    $item_details['lineItemPrice'] = $item['paid_price'];
                    $item_details['promotion'] = '';
                    $item_details['promotionDesc'] = '';
                    $item_details['LineItemApportionedValue'] = '';
                    $item_details['pickPriority'] = '';
                    $totalShipmentAmount = $totalShipmentAmount + floatval($item['paid_price']);

                    // get packsize ('variation') from simple attributes
                    $item_details['packSize'] = '';
                    if( ! empty($configAttributes['simples'][$item['sku']]['groupedattributes']['variation'])) {
                        $item_details['packSize'] = $configAttributes['simples'][$item['sku']]['groupedattributes']['variation'];
                    }

                    // get SAP article no
                    $item_details['sapArticleNo'] = '';
                    if( ! empty($configAttributes['simples'][$item['sku']]['groupedattributes']['sap_article_no'])) {
                        $item_details['sapArticleNo'] = $configAttributes['simples'][$item['sku']]['groupedattributes']['sap_article_no'];
                    }

                    // set bundle info
                    $itemTransferObj = $order->getItemCollection()[$id_soitem];
                    $bundleId = ($itemTransferObj) ? $itemTransferObj->getFkSkuBundle() : '';
                    $item_details['isBundle'] = ($bundleId) ? 1 : 0;
                    $item_details['bundleId'] = $bundleId;

                    // set delivery driver details during first iteration
                    if( ! isset($shipment_details['driverDetails'])) {
                        $driverDetails = [];
                        if($itemTransferObj) {
                            // delivery details by fk_sales_order_item_delivery
                            $deliveryId = $itemTransferObj->getFkSalesOrderItemDelivery();
                            $deliveryRow = $shipmentService->getDeliveryDetails($deliveryId);

                            $driverDetails['name'] = (isset($deliveryRow['delivery_boy_name'])) ? $deliveryRow['delivery_boy_name'] : '';
                            $driverDetails['phone'] = (isset($deliveryRow['delivery_boy_phone'])) ? $deliveryRow['delivery_boy_phone'] : '';;
                            // add to response
                            $shipment_details['driverDetails'] = $driverDetails;
                        }
                    }

                    //get categories [use memcache to get the ids]
                    $categoryService = new Catalog_Service_Categories();
                    $categoryModel = new Catalog_Model_Category();

                    $categoryIds = [];
                    $categories = []; // assign to the response
                    if( isset($configAttributes['meta']['categories']) ) {
                        $categoryIds = explode('|', $configAttributes['meta']['categories']);
                    } else { //alternate method query DB
                        $categoryIds = $categoryService->getCategoryIdsBySimpleSku($item['sku']);
                    }

                    if(count($categoryIds)) {
                        foreach ($categoryIds as $categoryId) {
                            $categoryRow = $categoryService->getCategoryById($categoryId);
                            $count = count($categoryModel->getParentCategoriesBySubcategoryId($categoryId));
                            $categories['L'.$count] = [
                                'id' => $categoryId,
                                'name' => $categoryRow['name']
                            ];
                        }
                    }
                    asort($categories);
                    $item_details['categories'] = $categories;

                    // set the UOM Type attribute
                    if( isset($configAttributes['groupedattributes']['uom_type']) ) {
                        $item_details['UOM'] = $configAttributes['groupedattributes']['uom_type'];
                    }
                    // end PKD-2078 changes

                    //Checking mandatory fields Packsize, UOm, atricle
                    if(empty($item_details['packSize']) || empty($item_details['sapArticleNo']) || empty($item_details['UOM'])){
                        $commonHelper = new Helpers_CommonHelper();
                        $skuDetails = $commonHelper->getPacksizeUomArticleBySKU($item_details['sku']);
                        if(!empty($skuDetails)){
                            $item_details['packSize'] = $skuDetails['name'];
                            $item_details['sapArticleNo'] = $skuDetails['sku_supplier_simple'];
                            $item_details['UOM'] = $skuDetails['uom'];
                        }

                    }

                    //PKD-4081
                    if(!empty($statusCode) && is_array($statusCode)){
                        $item_details['cancel_reason'] = $cancelReason->fetchCancelReason($item['id_sales_order_item'],$canceledStatus);
                    }
                }

                if(!empty($item_details)){
                    $items[] = $item_details;
                }
            }
        }

        $shipping['prefix'] =  $shipping_address['prefix'];
        $shipping['first_name'] =  $shipping_address['first_name'];
        $shipping['last_name'] =  $shipping_address['last_name'];
        $shipping['addressLine1'] =  $shipping_address['address1'];
        $shipping['addressLine2'] =  (!empty($shipping_address['address2']) ? $shipping_address['address2'] : '');
        $shipping['addressLine3'] =  (!empty($shipping_address['address3']) ? $shipping_address['address3'] : '');
        $shipping['city'] =  $shipping_address['city'];
        $shipping['state'] =  '';
        $shipping['postcode'] =  $shipping_address['postcode'];
        $shipping['phone'] =  $shipping_address['phone'];

        $billing['prefix'] =  $shipping_address['prefix'];
        $billing['first_name'] =  $shipping_address['first_name'];
        $billing['last_name'] =  $shipping_address['last_name'];
        $billing['addressLine1'] =  $shipping_address['address1'];
        $billing['addressLine2'] =  (!empty($shipping_address['address2']) ? $shipping_address['address2'] : '');
        $billing['addressLine3'] =  (!empty($shipping_address['address3']) ? $shipping_address['address3'] : '');
        $billing['city'] =  $shipping_address['city'];
        $billing['state'] =  '';
        $billing['postcode'] =  $shipping_address['postcode'];
        $billing['phone'] =  $shipping_address['phone'];

        $delivery_mode['HomeDelivery'] = ($pickupMethod['id_sales_order_pickup_method'] == 2 ? '1' : '0');
        $delivery_mode['PickInStore'] = ($pickupMethod['id_sales_order_pickup_method'] == 1 ? '1' : '0');

        if(!empty($deliverySlotData)){
            $DeliverySlot['deliverySlotId'] = $deliverySlotData['id_delivery_slot'];
            $DeliverySlot['date'] = $this->responseHelper->changeDateFormat($deliverySlotData['date']);
            $DeliverySlot['startTime'] = date('h:i a',strtotime($deliverySlotData['start_time']));
            $DeliverySlot['endTime'] = date('h:i a',strtotime($deliverySlotData['end_time']));
            $DeliverySlot['time'] = $DeliverySlot['startTime']."-".$DeliverySlot['endTime'];
        }else{
            $DeliverySlot['deliverySlotId'] = '';
            $DeliverySlot['date'] = '';
            $DeliverySlot['startTime'] = '';
            $DeliverySlot['endTime'] = '';
        }

        $shipment_details['shipmentNumber'] = $shipment['shipment_id'];
        $shipment_details['shipmentStatus'] = $shipment['status'];
        $shipment_details['order_number'] = $order_data['order_nr'];
        $shipment_details['order_id'] = $order_data['id_sales_order'];
        $shipment_details['order_date'] = $this->responseHelper->changeDateFormat($order_data['created_at']);
        $shipment_details['email'] = $order_data['customer_email'];
        $shipment_details['storeId'] = (isset($order_data['store_id']) ? $order_data['store_id']: '');

//        $result = $this->_getShipmentFormatStoreCode($shipment['shipment_id']);
        if(isset($result['store_format']) && isset($result['id_store'])){
            $shipment_details['storeId'] = $result['id_store'];
            $shipment_details['shipmentFormat'] = $result['store_format'];
            $shipment_details['logo'] = '';
            if(isset($shipment_details['shipmentFormat']) && $shipment_details['shipmentFormat'] == 'BB'){
                $bootstrap = Zend_Controller_Front::getInstance()->getParam('bootstrap');
                $shipment_details['logo'] = $bootstrap->getOption('BB_IMAGE_URL');
            }
        }
        $shipment_details['numberOfBags'] = '';

        $invoice_data=isset($shipment['shipment_id']) ?  $shipmentService->getInvoiceData($shipment['shipment_id']) : '';
        $shipment_details['invoiceNumber'] =  isset($invoice_data)? isset($invoice_data['invoice_nr']) ? $invoice_data['invoice_nr'] : '': ''; // PKD-2078
        $shipment_details['invoiceBarcode'] = isset($invoice_data)? isset($invoice_data['invoice_barcode']) ? $invoice_data['invoice_barcode'] : '': '';
        $shipment_details['specialInstruction'] = ''; // PKD-2078 @to-do
        $shipment_details['shippingAddress'] = $shipping;
        $shipment_details['billingAddress'] = $billing;
        $shipment_details['DeliveryMode'] = $delivery_mode;
        $shipment_details['DeliverySlot'] = $DeliverySlot;
        $shipment_details['total_item_count'] = count($items);
        $shipment_details['items'] = $items;

        $payment_data['payment_method'] = (strtolower($order_data['payment_method'])=='cashondelivery' ? 'COD': 'partrpay');
        if(count($items) > 0){
        //PKD-2049 populating grandtotal as per shipment details
            $payment_data['shipmentSubTotal'] = "Rs. ".number_format($totalShipmentAmount, 2,'.', '');
        }else{
            $payment_data['shipmentSubTotal'] = "Rs. ".'0';
        }

        if ($invoice_barcode_flag) {
            $ret_array = array('ShipmentDetails' => $shipment_details, 'payment' => $payment_data, 'invoice_barcode' => true);
        } else {
            $ret_array = array('ShipmentDetails' => $shipment_details, 'payment' => $payment_data);
        }

         return $ret_array;
    } // end: function _createShipmentRowData

    public function getStatusList() {
        $itemStatusListObj = new OrderprocessingExt_Model_Order_Item_Status();
        return $itemStatusListObj->getOrderItemStatus();
    }
    public function getStatus($id){
        $statuslist = $this->getStatusList();
        foreach($statuslist as $status){
            if($status['id_sales_order_item_status'] == $id){
                return $status['en_display_name'];
            }
        }
    }

    public function setCancelReason($data){
        $salesOrderShipment = new DbTable_Sales_Order_ShipmentTable;
        $updateFields = array(DbTable_Sales_Order_ShipmentRow::CANCEL_REASON => $data['cancelReason'] , DbTable_Sales_Order_ShipmentRow::STATUS => 9);
        $where = array(DbTable_Sales_Order_ShipmentRow::FK_SALES_ORDER . ' = ? ' => $data['orderId']);
        $res = $salesOrderShipment->update($updateFields, $where);
        return $res;
    }

    public function getCancellationReason($order_id){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()->from(array('s' => 'sales_order_shipment'),array('status','cancel_reason'));
        $select->where("s.fk_sales_order = '$order_id'");
        return $db->fetchRow($select);
    }

    public function getEanBySku($sku, $storeId = "")
    {
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()->from(array('cs' => 'catalog_simple'), array('se.ean'))
                ->join(array('se' => 'skutostore_ean'), 'cs.id_catalog_simple=se.fk_catalog_simple', array('se.ean'));
        $select->where('cs.sku IN("' . $sku . '")');
        $result = $db->fetchAll($select);
        if (empty($result) && !empty($storeId)) {
            return $this->getEanBySkutoPrice($sku, $storeId);
        }
        return $result;
    }

    //fetching ean from skutostore_price table
    public function getEanBySkutoPrice($sku,$storeId) {
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()->from(array('cs' => 'catalog_simple'),array('ssp.ean'))
                //->join(array('se'=>'skutostore_ean'),'cs.id_catalog_simple=se.fk_catalog_simple',null)
                ->join(array('ssm'=>'skutostore_mapping'),'cs.id_catalog_simple=ssm.fk_catalog_simple',null)
                ->join(array('ssp'=>'skutostore_price'),'ssm.id_skutostore_mapping=ssp.fk_skutostore_mapping',null);
        $select->where('cs.sku IN("'.$sku.'")');
        $select->where('ssp.to_date >= "'.date('Y-m-d').'"');
        //$select->where("ssp.ean = se.ean");
        $select->where('ssm.fk_store =?', $storeId);
        $select->where("ssp.is_active = '1'");
        return $db->fetchAll($select);
    }

    /** PKD-259
     * @desc: method to get all items by shipmentId
     * @param type $shipmentId
     * @return type
     */
    public function getItemsByShipmentItems($shipmentId = null){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('s' => 'sales_order_shipment'),
                    array('shipment_grandtotal','shipment_charge','shipment_id','order_nr','i.status','fk_sales_order','fk_delivery_store_slot','fk_sales_order_pickup_method'))
                ->join(array('i'=>'sales_order_shipment_item'),'s.id_sales_order_shipment = i.fk_sales_order_shipment',
                        array('i.fk_sales_order_item'))
                ->join(array('si'=>'sales_order_item'), 'i.fk_sales_order_item = si.id_sales_order_item',array('si.fk_sales_order_item_status','quantity','sku'))
                ->where("s.shipment_id = '$shipmentId'");
        return $select->query()->fetchAll();
    }


    /**
     *
     * @param type $shipmentId
     * @return type
     */
    public function getRejectItemsByShipmentItems($shipmentId = null){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('s' => 'sales_order_shipment'),
                    array('s.order_nr'))
                ->join(array('o' => 'sales_order'),'s.fk_sales_order = o.id_sales_order',
                    array('o.id_sales_order','o.store_id','o.fk_sales_order_address_billing','o.fk_sales_order_address_shipping','o.payment_method','o.fk_customer','s.shipment_grandtotal','s.shipment_charge','s.shipment_id','i.status','s.fk_sales_order','s.fk_delivery_store_slot','s.fk_sales_order_pickup_method'))
                ->join(array('i'=>'sales_order_shipment_item'),'s.id_sales_order_shipment = i.fk_sales_order_shipment',
                        array('i.fk_sales_order_item'))
                ->join(array('si'=>'sales_order_item'), 'i.fk_sales_order_item = si.id_sales_order_item',array('si.fk_sales_order_item_status'))
                ->join('sales_order_item_status_history', 'sales_order_item_status_history.fk_sales_order_item = si.id_sales_order_item', array(''))
                ->join('sales_order_item_status', 'sales_order_item_status.id_sales_order_item_status = sales_order_item_status_history.fk_sales_order_item_status', array(''))
                ->where("(sales_order_item_status.name in ('rejected','ex_loss'))")
                ->where("s.shipment_id = '$shipmentId'")
                ->group('i.fk_sales_order_item');

        return $select->query()->fetchAll();
    }

    public function fetchRejectExLossItemsByShipmentId($shipmentId){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('o' => 'sales_order'),
                    array('order_nr','id_sales_order','store_id','fk_sales_order_address_billing','fk_sales_order_address_shipping','payment_method','fk_customer'))
                ->join(array('s' => 'sales_order_shipment'),'s.fk_sales_order = o.id_sales_order',
                    array('s.shipment_grandtotal','s.shipment_charge','s.shipment_id','i.status','fk_sales_order','fk_delivery_store_slot','fk_sales_order_pickup_method'))
                ->join(array('i'=>'sales_order_shipment_item'),'s.id_sales_order_shipment = i.fk_sales_order_shipment',
                        array('i.fk_sales_order_item'))
                ->join(array('si'=>'sales_order_item'), 'si.fk_sales_order= o.id_sales_order and i.fk_sales_order_item = si.id_sales_order_item',array('si.id_sales_order_item'))
                ->join(array('sh'=>'sales_order_item_status_history'), 'sh.fk_sales_order_item = si.id_sales_order_item', array('fk_sales_order_item_status'))
                ->join('sales_order_item_status', 'sales_order_item_status.id_sales_order_item_status = sh.fk_sales_order_item_status', array('item_status' => 'sales_order_item_status.name'))
                ->where("(sales_order_item_status.name in ('rejected','ex_loss'))")
                //->where("o.order_nr = '$orderNr'")
                ->where("s.shipment_id = '$shipmentId'")
                ->group('si.id_sales_order_item');

        return $select->query()->fetchAll();
    }



    public function getRejectForInvoice($data, $rejectedItems, $invoiceType) {
        try {
            $fileName = "reject_invoice_debug.log";
            $returnValue = array('status' => false);
            $returns['result'] = $rejectedItems;
            $OrdExtService = new OrderprocessingExt_Service_Orderprocess();
            $OrdExtModel = $OrdExtService->getOrderExtModel();

            if(!empty($returns['result'])){
                $returnItems = $this->getAttachmentModel()->attachAdditionalDataForRejectInvoiceGeneration($returns);
            }else{
                return false;
            }

            Bob_Log::debug('attached : '.var_export($returnItems,true), $fileName);
            if(!empty($returnItems)){
                $storeMasterService = new Storemaster_Service_StoremasterAddress();
                $storeData = $storeMasterService->getStoreByStoreId($returns['result'][0]['store_id']);
                //Bob_Log::debug('storeData : '.var_export($storeData,true), $fileName);
                $returns['store'] = $storeData;
                $returns['billing_address'] = $returnItems['billing_address'];
                $returns['shipping_address'] = $returnItems['shipping_address'];

                foreach ($returns['result'] as $key => $rec){
                    $returns['result'][$key]['payment_method'] = (strtolower($rec['payment_method'])=='cashondelivery' ? 'COD': $rec['payment_method']);
                }
                $returns['items'] = $returnItems['items'];
                Bob_Log::debug('returns[items] : '.var_export($returns['items'],true), $fileName);
                if (is_array($returns['items']) && count($returns['items']) > 0) {
                    $catalogConfService    = new CatalogExt_Service_Config();
                    $catalogConfModel = $catalogConfService->getCatalogConfigModel();
                    $catalogConfArray  = $catalogConfModel->getHSNForReturn($returns['items']);
                    //Bob_Log::debug('catalogConfArray : '.var_export($catalogConfArray,true), $fileName);
                    if(!empty($catalogConfArray)){
                        $localStoreObj = new Api_Service_Service();
                        $localStorageAdapter = $localStoreObj->getStorageAdapter();
                        $hsn = array();
                        foreach($catalogConfArray as $k=>$v){
                            $key = $v['sku']."_".$storeData['store_code'];
                            $configAttributes = $localStorageAdapter->get('skustore', $key);
                            $productData = $localStorageAdapter->get('product', $v['sku']);

                            // Assign the sku-to-store simple values into the appropriate keys in product data simples
                            if (!empty($productData) && !empty($configAttributes)) {
                                foreach ($productData['simples'] as $simpleSku => &$simpleData) {
                                    if (isset($configAttributes['simples'][$simpleSku])) {
                                        $skuStoreSimple = $configAttributes['simples'][$simpleSku];
                                        if (isset($skuStoreSimple['meta'])) {
                                            $simpleData['meta']['price']            = $skuStoreSimple['meta']['price'];
                                            $simpleData['meta']['special_price']    = $skuStoreSimple['meta']['special_price'];
                                            $simpleData['meta']['member_price']     = $skuStoreSimple['meta']['member_price'];
                                            $simpleData['meta']['saleablequantity'] = $skuStoreSimple['meta']['saleablequantity'];
                                            $simpleData['meta']['quantity']         = $skuStoreSimple['meta']['quantity'];
                                            $simpleData['meta']['threshold']        = $skuStoreSimple['meta']['threshold'];
                                        }
                                    }
                                }
                                $configAttributes = $productData;
                            }

                            $hsn[$v['sku']]['simples'] = $configAttributes['simples'];


                            if($v['fk_catalog_attribute_option_global_uom_type'] != NULL){
                                $fk_uom = $v['fk_catalog_attribute_option_global_uom_type'];
                                $hsn[$v['sku']]['uom'] = $catalogConfModel->getUOM($fk_uom);
                            }
                        }
                    }

                    $id_sales_order = '';
                    $newHsn = array();
                    $newEAN = array();
                    $eanArray = array();

                    foreach ($returns['items'] as $i => $return) {
                        $eodStoreCode = $return['store_code'];
                        $lineItemIds[] = $return['id_sales_order_item'];
                        $api = new Catalog_Service_Product();
                        $config = $api->getConfigurableBySimpleSku($return['sku']);
                        $simple = $api->getSimpleBySku($return['sku']);
                        $newHsn[$config['sku']]['sku'] = $config['sku'];

                        $newHsn[$config['sku']]['packSize'] = $hsn[$config['sku']]["simples"][$return['sku']]['groupedattributes']['variation'];

                        $apiBrand = new Catalog_Service_Brand();
                        $brand = $apiBrand->getBrandNameBySimpleSku($simple['sku']);


                        $configSku = explode('-',$simple['sku'])[0];

                        $returns['items'][$i]['sap_article'] = $simple['sap_article_no'];
                        $returns['items'][$i]['brand'] = $brand;
                        $h = "";
                        foreach ($catalogConfArray as $catalog){
                            if($catalog['sku'] == $configSku){
                                $h = $catalog['hsn'];
                                break;
                            }
                        }
                        $returns['items'][$i]['hsn'] = $h;
                        $newHsn[$config['sku']]['hsn'] = $h;
                        $newHsn[$config['sku']]['uom'] = $hsn[$config['sku']]['uom'];
                        $newEAN[$simple['fk_catalog_simple']]['sku'] = $simple['sku'];
                        $newEAN[$simple['fk_catalog_simple']]['ean'] = "";
                        $eanArray[$simple['sku']] = $simple['fk_catalog_simple'];

                    } // end: foreach
                    $EAN = $this->getEan($eanArray, $newEAN);

                    $returns['EAN'] = $EAN;
                    $returns['HSN'] = $newHsn;
                    $taxMasterService = new TaxMaster_Service_TaxCalculate();
                    $taxMasterModel  = $taxMasterService->getTaxCalculateModel();
                    $return['store_code'] = isset($storeData['store_code']) ? $storeData['store_code']:'';
                    $itemsWithTax = $taxMasterModel->taxCalculationForReturns($returns);
                    //Bob_Log::debug('itemsWithTax : '.var_export($itemsWithTax,true), $fileName);
                    if(!empty($itemsWithTax)){
                        $returns = $itemsWithTax;
                    }

                    $commonTaxes = array();
                    foreach ($returns['items'] as $item){
                        if(count($commonTaxes)){
                            $taxKeys = array_keys($item['tax']);
                            foreach($taxKeys as $taxKey){
                                if(isset($commonTaxes[$item['tax'][$taxKey]['print_code']]['print_code']) && $item['tax'][$taxKey]['print_code'] == $commonTaxes[$item['tax'][$taxKey]['print_code']]['print_code']){
                                    $commonTaxes[$item['tax'][$taxKey]['print_code']]['tax_amount'] += $item['tax'][$taxKey]['tax_amount'];
                                    $commonTaxes[$item['tax'][$taxKey]['print_code']]['tax_net_price'] += $item['tax'][$taxKey]['tax_net_price'];
                                } else {
                                    $commonTaxes[$item['tax'][$taxKey]['print_code']] = $item['tax'][$taxKey];
                                }
                            }
                        } else {
                            $taxKeys = array_keys($item['tax']);
                            foreach($taxKeys as $taxKey){
                                $commonTaxes[$item['tax'][$taxKey]['print_code']] = $item['tax'][$taxKey];
                            }
                        }
                    }
                    $returns['commonTax'] = $commonTaxes;

                    $StoreEmpService =  new Storeemployee_Service_Employee();
                    $storeEmpDetails = $StoreEmpService->getEmployeeDetails($data['emp_code']);
                    if(empty($storeEmpDetails)) {
                        return [
                            'message' => 'Employee does not exist',
                            'status' => false
                        ];
                    }
                    //Bob_Log::debug('$storeEmpDetails : '.var_export($storeEmpDetails,true), $fileName);

                    $invoice_details = $OrdExtModel->generateRejectInvoiceNumberAndBarcode($returns, $storeEmpDetails, $data['shipment_id']);

                    //Bob_Log::debug('invoice_details : '.var_export($invoice_details,true), $fileName);
                    if(!empty($invoice_details)){
                        $returns['invoice_details'] = $invoice_details;
                    }

                    $db = Zend_Db_Table_Abstract::getDefaultAdapter();
                    $select = $db->select();
                    $select->from('customer_loyalty', array('loyalty_id' => 'loyalty_id'))
                        ->where('fk_customer = ?', $returns['result'][0]['fk_customer']);
                    $loyalty_id = $db->fetchOne($select);

                    $customer = new CustomerExt_Model_CustomerApi();
                    $customerInfo = $customer->getDetailsByCustomerIdApi($returns['result'][0]['fk_customer']);
                    //Bob_Log::debug('customerInfo : '.var_export($customerInfo,true), $fileName);

                    $customer_info = array();
                    if(isset($customerInfo['details']) && !empty($customerInfo['details'])) {
                        $shipment = explode("_", $data['shipment_id']);
                        $delivery_mode = "";
                        if($shipment[1] == 1){
                            $delivery_mode = "In-Store Pick-up";
                        } else if ($shipment[1] == 2) {
                            $delivery_mode = "Standard Delivery";
                        } else if ($shipment[1] == 3) {
                            $delivery_mode = "Express Delivery";
                        } else if ($shipment[1] == 4) {
                            $delivery_mode = "Self Checkout";
                        }
                        $customer_info = array(
                            "pick_up_method" => $delivery_mode,
                            "payment_method" => "",
                            "customer_name" => $customerInfo['details'][0]['firstName']." ".$customerInfo['details'][0]['lastName'],
                            "contact_number" => $customerInfo['details'][0]['contact_number']
                        );
                    }

                    $returns['customer_info'] = $customer_info;

                    $params = array(
                        "cardNo" => $loyalty_id,
                        "storeCode" => $returns['store']['store_code'],
                        "mobileNo" => $customerInfo['details'][0]['contact_number']
                    );

                    $loyaltyService = new Loyalty_Service_Loyalty();
                    $loyaltyMembership = $loyaltyService->getMembershipDetails($params);
                    if(isset($loyaltyMembership) && !empty($loyaltyMembership) && $loyaltyMembership['responseCode'] == 200){
                        $returns['customerLoyalty'] = $loyaltyMembership;
                    }


                    $orderCollection = $OrdExtModel->getOrderDetailByOrderIdAndStatus(array("orderId"=>$returns['result'][0]['id_sales_order']));

                    $returns['delivery_charge'] = $rejectedItems[0]['shipment_charge']??0;
                    $returns['shipment_grandtotal'] = $rejectedItems[0]['shipment_grandtotal']??0;
                    $returns['shipment_id'] = $rejectedItems[0]['shipment_id'];
                    $returns['customer_info']['payment_method'] = $returns['result'][0]['payment_method'];
                    $returns = $this->changingTheQuantityForWeight($returns);
                    //Bob_Log::debug('returns : '.var_export($returns,true), $fileName);
                    $this->saveRejectInvoice($returns, $eodStoreCode, $lineItemIds, $invoiceType);
                    if (!isset($returns)) {
                        $returnValue['message'] = 'Rejects not available against this item ids';
                        $returnValue['status'] = false;
                    } else {
                        $returnValue['data'] = $returns;
                        $returnValue['status'] = true;
                    }
                } else {
                    $returnValue['message'] = 'Rejects not available for the given input';
                    $returnValue['status'] = false;
                }
            }else{
                $returnValue['message'] = 'Rejects not available for the given input';
                $returnValue['status'] = false;
            }
            return $returnValue;

        } catch (Exception $e) {
            Bob_Log::debug('Error : '.var_export($e->getMessage(),true), $fileName);
            Bob_Log::gelfLog([$e, 'ERROR', __METHOD__]);
            Bob_Log::event('failed: Exception happened ' . print_r($e->getTraceAsString(), true));
            //$this->_handleReturnException($e);
        }
    }
    public function getEan($skuArray,$skuIdArray)
    {
        $adapter = Zend_Db_Table::getDefaultAdapter();
        $select = $adapter->select();
        $fields=array('fk_catalog_simple','ean');
        $select->from('skutostore_ean', $fields);
        $select->where('is_active = ?', '1');
        $select->where('fk_catalog_simple IN(?)', $skuArray);
        $row = $adapter->fetchAll($select);
        foreach($row as $k=>$v)
        {
            $skuIdArray[$v['fk_catalog_simple']]['ean']=$v['ean'];
        }
        return $skuIdArray;
    }

    public function getDeliveryStoreSlotById($order_id){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()->from(array('s' => 'sales_order_shipment'),array('fk_delivery_store_slot','fk_sales_order_pickup_method'));
        $select->where("s.fk_sales_order = '$order_id'");
        return $select->query()->fetchAll();
    }

    public function getShipmentIds($ids){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()->from(array('s' => 'sales_order_shipment_item'),array('fk_sales_order_shipment','fk_sales_order_item'));
        $select->where("s.fk_sales_order_item IN (?)",$ids);
        return $select->query()->fetchAll();
    }

    public function getShipmentMethods($ids){
        $db = Zend_Db_Table::getDefaultAdapter();
        $result=array();
        if(count($ids)>0)
        {
        $select = $db->select()->from(array('s' => 'sales_order_shipment'),array('id_sales_order_shipment','fk_sales_order_pickup_method','shipment_id','status'));
        $select->where("s.id_sales_order_shipment IN (?)",$ids);
        $result=$select->query()->fetchAll();
        }
        return $result;
    }

    /**
     * Get Shipment ID using the Order Item ID
     *
     * @param  int $orderItemId id_sales_order_item
     * @return string $shipmetId shipment_id
     */
    public function getShipmentIdByOrderItemId($orderItemId) {
        $adapter = Zend_Db_Table::getDefaultAdapter();

        $select = $adapter->select();
        $select->from(['ss' => 'sales_order_shipment'], ['shipment_id' => 'ss.shipment_id']);
        $select->join(['si' => 'sales_order_shipment_item'], 'si.fk_sales_order_shipment = ss.id_sales_order_shipment',[]);
        $select->where('si.fk_sales_order_item = ?', $orderItemId);

        $shipmetId = $adapter->fetchRow($select);
        return ( ! empty($shipmetId['shipment_id'])) ? $shipmetId['shipment_id'] : false;
    }

    /**
     * @author Jyoti.Jakhmola <jyoti.jakhmola@embitel.com>
     * @task PKD-946 - method to sen the shipments to the picker app queue
     * @param type $orderId integer mandatory
     */
    public function sendOrdersToPickerAppQueue($orderId) {
        try {
            Bob_Log::debug('----SendOrdersToPickerAppQueue orderId= '. $orderId, 'pickerQueue.log');
            $shipmentTable = new DbTable_Sales_Order_ShipmentTable();
            $orderShipmentList = $shipmentTable->findAllByCriteria(array(DbTable_Sales_Order_ShipmentRow::FK_SALES_ORDER => (int) $orderId));
            if(!empty($orderShipmentList)) {
                foreach ($orderShipmentList as $columnName => $shipment) {
                    if (!empty($shipment['shipment_id']) && $shipment['fk_sales_order_pickup_method'] != 4) {
                        if ($shipment['store_code'] != '9052') {
                            $awsSqsObj = new AwsSqs();
                            $queue_url = $awsSqsObj->getSqsQueUrlByName($awsSqsObj->_sqsShippmentQue);
                            if (!empty($queue_url)) {
                                $messageParams = $awsSqsObj->getMessageParams(array($shipment['shipment_id']), $queue_url, 10, "Shippment Ids", "PKD");
                                Bob_Log::debug('Pushed to Queue = ' . var_export($messageParams, true), 'pickerQueue.log');
                                $awsSqsObj->sqsSendMessage($messageParams);
                            } else {
                                Bob_Log::debug('Queue url missing- ' . var_export($queue_url, true), 'pickerQueue.log');
                            }
                        } else {
                            Bob_Log::debug('Gift voucher order = ' . $shipment['shipment_id'], 'pickerQueue.log');
                            $shipmentService = new OrderprocessingExt_Service_Shipment();
                            $shipmentService->getShipmentAck($shipment['shipment_id']);
                        }
                    } else {
                        Bob_Log::debug('ShipmentDetails not sent to queue : ' . var_export($shipment, true), 'pickerQueue.log');
                    }
                }
            } else {
                Bob_Log::debug('No Order found for - '.$orderId.' -> '. var_export($orderShipmentList,true), 'pickerQueue.log');
            }
        } catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug(__FUNCTION__.' EXCEPTION : '. var_export($exceptionMsg,true), 'pickerQueue.log');
        }
    }

    /**
     * PKD-3318: Get data for push notification
     *
     * @return array notification data
     */
    public function getShipmentNotificationData() {
        // Get configured interval for push notifications
        $config = new ConfigurationExt_Service_Loader_Namespace('orderprocessing-ext', true);
        $interval = $config->getValue('shipment_notification_interval');
        $interval = !empty($interval) ? $interval : 45;

        $dbAdapter = Zend_Db_Table::getDefaultAdapter();
        $select = $dbAdapter->select();

        $select->from(['SOS' => 'sales_order_shipment'], [
            'id_sales_order_shipment',
            'shipment_ids' => new Zend_Db_Expr('GROUP_CONCAT(`SOS`.`shipment_id`)'),
            'id_sales_order' => 'fk_sales_order',
            'order_nr',
            'id_delivery_store_slot' => 'fk_delivery_store_slot'
        ]);

        $select->join(['SO'=>'sales_order'], 'SO.'.DbTable_Sales_OrderRow::ID_SALES_ORDER.' = SOS.'.  DbTable_Sales_Order_ShipmentRow::FK_SALES_ORDER, ['store_id']);
        $select->join(['SOI'=>'sales_order_item'], 'SOI.'.DbTable_Sales_Order_ItemRow::FK_SALES_ORDER.' = SO.'.DbTable_Sales_OrderRow::ID_SALES_ORDER, []);
        $select->join(['DSS'=>'delivery_store_slots'], 'DSS.'.DbTable_Delivery_Store_SlotsRow::ID_DELIVERY_STORE_SLOT.' = SOS.'.DbTable_Sales_Order_ShipmentRow::FK_DELIVERY_STORE_SLOT, []);
        $select->join(['DS'=>'delivery_slots'], 'DS.'.DbTable_Delivery_SlotsRow::ID_DELIVERY_SLOT.' = DSS.'.DbTable_Delivery_Store_SlotsRow::FK_DELIVERY_SLOT, []);
        $select->join(['DSC'=>'delivery_slots_calender'], 'DSC.'.DbTable_Delivery_Slots_CalenderRow::ID_DELIVERY_SLOT_CALENDER.' = DSS.'.DbTable_Delivery_Store_SlotsRow::FK_DELIVERY_SLOT_CALENDER, []);

        $select->where('SOI.'.DbTable_Sales_Order_ItemRow::FK_SALES_ORDER_ITEM_STATUS." = ?", 4);
        $select->where('DSC.'.DbTable_Delivery_Slots_CalenderRow::DATE." <= CURRENT_DATE()");
        $select->where('DS.'.DbTable_Delivery_SlotsRow::START_TIME. ' <= TIME(NOW() + INTERVAL ' . $interval . ' MINUTE)');
        $select->group('SO.store_id');

        $results = $dbAdapter->fetchAll($select);
        return $results;
    }

    public function shipmentToUpdateQueue($data = []) {
        if (strtolower($data['status']) == 'delivered' || strtolower($data['status']) == 'failed') {
            try {
                $shipmentId = explode('_', $data['shipmentId']);
                if (strtolower($data['status']) == 'failed' && $this->isGiftVoucherOrder($shipmentId[0])) {
                    return ['responseCode' => 412, 'responseMessage' => 'Unable to cancel this order' . $data['shipmentId']];
                }

                $saveForFeedback = isset($data['saveForFeedback']) ? $data['saveForFeedback'] : true;
                $newShipmentId = $data['shipmentId'];
                if(count($shipmentId) == 3){
                    if(!$shipmentId[1] == 6 || !$shipmentId[2] == 1){
                        return ['responseCode' => 412, 'responseMessage' => 'Invalid ShipmentId ' . $data['shipmentId']];
                    }else{
                        unset($shipmentId[2]);
                        $newShipmentId = implode('_', $shipmentId);
                    }
                }else if(count($shipmentId) > 3){
                    return ['responseCode' => 412, 'responseMessage' => 'Invalid ShipmentId ' . $data['shipmentId']];
                }

                $all_items = $this->getItemsByShipmentId($newShipmentId);
                if (!empty($all_items)) {
                    $service = new ConfigurationExt_Service_Configuration();
                    $config = $service->getConfigurationConfig();
                    $allowedDeliveryMethods = explode(',', $config->allowed_delivery_methods_to_shipment_queue);

                    //Enable below lines if Queue consumes lineitem-level data
                    //$result = array();
                    //foreach ($all_items as $item) {
                    //    if(in_array($item['fk_sales_order_pickup_method'], $allowedDeliveryMethods)){
                    //        $result[] = $this->_pushShipmentToUpdateShipmentStatusQueue($item, $data['status']);
                    //    }
                    //    return ['responseCode' => 412, 'responseMessage' => 'Cannot move order when delivery method is ' . $item['fk_sales_order_pickup_method'] . '. Please change config in Configuration-ext'];
                    //}
                    //return $result;

                    if(in_array($all_items[0]['fk_sales_order_pickup_method'], $allowedDeliveryMethods)){
                       return $this->_pushShipmentToUpdateShipmentStatusQueue($all_items[0], $data['status'],$data['shipmentId'],$saveForFeedback);
                    }

                    return ['responseCode' => 412, 'responseMessage' => 'Cannot move '.$data['shipmentId'].' when delivery method is ' . $all_items[0]['fk_sales_order_pickup_method'] . '. Please change config in Configuration-ext'];

                }

                return ['responseCode' => 412, 'responseMessage' => 'Record not found for ShipmentId ' . $data['shipmentId']];

            } catch (Exception $ex) {
                $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
                Bob_Log::debug(__FUNCTION__ . ' EXCEPTION : ' . var_export($exceptionMsg, true), 'API/queue.log');
                return ['exception'=>$ex->getMessage()];
            }
        }
        return ['responseCode' => 412, 'responseMessage' => 'Status should be delivered or failed'];
    }

    protected function isGiftVoucherOrder($orderNumber)
    {
        try {
            $db = Zend_Db_Table_Abstract::getDefaultAdapter();
            $select = $db->select();
            $select->from('gift_voucher', array('id_gift_voucher', 'coupon_code', 'pin', 'article_no'));
            $select->where('order_nr  = ?', $orderNumber);
            $gvCouponsArr = $db->fetchAll($select);
            if (isset($gvCouponsArr) && count($gvCouponsArr) > 0) {
                return true;
            }
        } catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug(__FUNCTION__ . ' Exception : ' . var_export($exceptionMsg, true), 'shipment_status_change.log');
        }

        return false;
    }

    private function _pushShipmentToUpdateShipmentStatusQueue($item = [], $status, $shipmentId, $saveForFeedback) {
        try{
            $awsSqsObj = new AwsSqs();
            $queue_url = $awsSqsObj->getSqsQueUrlByName($awsSqsObj->_sqsUpdateLmdQue);

            $data = array("shipment_id" => $shipmentId,
                "itemSequenceId" => $item['fk_sales_order_item'],
                "status" => $status,
                "saveForFeedback" => $saveForFeedback,
                "reason" => "");

            $messageParams = $awsSqsObj->getMessageParams($data, $queue_url, 10, "Manual pushing", "PickerApp");

            return $awsSqsObj->sqsSendMessage($messageParams)->toArray();

        }catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug(__FUNCTION__ . ' EXCEPTION : ' . var_export($exceptionMsg, true), 'API/queue.log');
            return ['exception'=>$ex->getMessage()];
        }
    }

    public function orderSearch($data = []) {
        try {
            $adapter = Zend_Db_Table::getDefaultAdapter();
            $select = $adapter->select();

            if(empty($data['searchType'])){
                return ['responseCode' => 412, 'responseMessage' => 'searchType should not be empty'];
            }

            if(empty($data['searchValue'])){
                return ['responseCode' => 412, 'responseMessage' => 'searchValue should not be empty'];
            }

            //Validating Search Type
            if(strtolower($data['searchType']) != 'phoneno' && strtolower($data['searchType']) != 'orderno'){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid search type : ' . $data['searchType']];
            }

            //Validating Search value
            if(!is_numeric($data['searchValue'])){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid search value : ' . $data['searchValue']];
            }else if(strtolower ($data['searchType']) == 'phoneno'){
                if(strlen($data['searchValue']) != 10){
                    return ['responseCode' => 412, 'responseMessage' => 'Invalid Phone number : ' . $data['searchValue']];
                }
            }

            $searchKey = $data['searchValue'];
            $phone = (strtolower($data['searchType']) == 'phoneno') ? true : false;

            $pageSize = (isset($data['pageSize'])) ? $data['pageSize'] : 10;
            $pageNo = (isset($data['pageNo'])) ? $data['pageNo'] : 1;

            if($pageNo <= 0){
                return ['responseCode' => 412, 'responseMessage' => 'pageNo should be greater than 0'];
            }

            if($pageSize <= 0){
                return ['responseCode' => 412, 'responseMessage' => 'pageSize should be greater than 0'];
            }

            $pageSize = is_numeric($pageSize) ? (int)$pageSize : 10;  //10 Default page size
            $pageNo = is_numeric($pageNo) ? ((int)$pageNo == 1 ? 0 : ((int)$pageNo - 1) * $pageSize ) : 0;  //11 Default page no

            if($pageSize > 20){
                return ['responseCode' => 412, 'responseMessage' => 'pageSize should be less than 20'];
            }

            $bbstoreCode = false;
            if(!empty($data['storeCode'])){
                if(!is_numeric($data['storeCode'])){
                    return ['responseCode' => 412, 'responseMessage' => 'storeCode should be an integer'];
                }
                $storeAddressModel =  new Storemaster_Service_StoremasterAddress();
                $storeDetails = $storeAddressModel->getStoreByStoreCode(['storeCode' => $data['storeCode']]);
                if(!empty($storeDetails) && $storeDetails['store_format'] != 'ED'){
                    $bbstoreCode = (int)$data['storeCode'];
                }
            }

            if($phone){
                //Get orders from searchOrderByContactNumber()
                $result = $this->searchOrderByContactNumber($searchKey, $pageNo, $pageSize, $bbstoreCode);
            }else{
                //Get orders from searchOrderByOrderNumber()
                $result = $this->searchOrderByOrderNumber($searchKey, $pageNo, $pageSize, $bbstoreCode);
            }

            if(!empty($result['orders']) && is_array($result['orders'])){
                foreach ($result['orders'] as &$order) {
                    if($order['paymentMethod'] == 'CashOnDelivery'){
                        $order['eligibleForReturn'] = ($order['status'] == 'cash_declared' && !$bbstoreCode) ? true : false;
                        $order['eligibleForInward'] = true;
                    }else{
                        $order['eligibleForReturn'] = ($order['status'] == 'delivered' && !$bbstoreCode) ? true : false;
                        $order['eligibleForInward'] = true;
                    }
                }
            }

            return $result;

        } catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug('orderSearch()  EXCEPTION :' . var_export($exceptionMsg, true), 'orderSearch.log');
            return ['responseCode' => 500, 'responseMessage' => 'Exception while fetching orders'];
        }
    }

    /**
     * @author Thejas N <thejas.nagabusan@embitel.com>
     * @task PKD-5995 - shipment ACK
     * @param type $shipmentID string mandatory
     */
    public function shipmentAck($shipmentId) {
        try{
            $currentDate = date('Y-m-d');
            $currentTime = date("H:i:s");
            $orderModel = new Orderprocessing_Model_Order();
            $shipment = explode('_', $shipmentId);
            $order_nr = $shipment[0];

            try{
                $order = $orderModel->findOrderByOrderNr($order_nr, true)->toArray();
                $orderItems = $order['item_collection'];
            }catch(Exception $e){
                Bob_Log::debug('Exception while ACK Shipment ' . var_export($e->getMessage(), true), 'shipmentAck'.$currentDate.'.log');
                return ['responseCode' => 412, 'responseMessage' => 'Invalid shipmentId'];
            }

            $shipmentStatus = new Erp_Service_Erp();
            $exportedStatus = [];

            foreach ($orderItems as $item) {
                if($item['fk_sales_order_item_status'] == 3){
                    $changeStatus = $shipmentStatus->setOrderStatus($order_nr, $item['id_sales_order_item'], 'exported', $currentDate, $currentTime, 'PickerApp');
                    $exportedStatus[$item['id_sales_order_item']] =  $changeStatus;
                    Bob_Log::debug($item['id_sales_order_item'].' status changed --- ' . var_export($changeStatus,true), 'shipmentAck'.$currentDate.'.log');
                   }else{
                       $exportedStatus[$item['id_sales_order_item']] =  'not in exportable status';
                    Bob_Log::debug($order_nr.' no items found in exportable status ', 'shipmentAck'.$currentDate.'.log');
                }
            }
            return ['responseCode' => 200, 'responseMessage' => 'Ack received', 'status'=> $exportedStatus];
        }catch(Exception $e){
            Bob_Log::debug('Exception while ACK Shipment ' . var_export($e->getMessage(), true), 'shipmentAck'.$currentDate.'.log');
            return ['responseCode' => 500, 'responseMessage' => 'Exception while shipment details'];
        }
    }

    /**
     * @author Thejas N <thejas.nagabusan@embitel.com>
     * @task PKD-6241 - Order Search
     * @param type $contactNumber string mandatory
     * @param type $pageNo int mandatory
     * @param type $pageSize int mandatory
     */
    public function searchOrderByContactNumber($contactNumber, $pageNo, $pageSize, $bbstoreCode = null) {
        $adapter = Zend_Db_Table::getDefaultAdapter();
        $select = $adapter->select();

        $columns = $this->_orderSearchColumns();

        $CCN = $adapter
                    ->select()
                    ->from('customer_contact_number')
                    ->where('contact_number  =?',$contactNumber);

        $select->from(array('ccn' => $CCN), $columns)
                ->joinLeft(['so' => 'sales_order'], 'ccn.fk_customer = so.fk_customer', NULL);

        if($bbstoreCode){
            $select->joinLeft(['sos' => 'sales_order_shipment'], 'so.id_sales_order = sos.fk_sales_order AND sos.store_code ='.$bbstoreCode, NULL);
        }else{
            $select->joinLeft(['sos' => 'sales_order_shipment'], 'so.id_sales_order = sos.fk_sales_order', NULL);
        }

        $select->joinLeft(['dss' => 'delivery_store_slots'], 'sos.fk_delivery_store_slot = dss.id_delivery_store_slot', NULL)
            ->joinLeft(['dsc' => 'delivery_slots_calender'], 'dsc.id_delivery_slot_calender = dss.fk_delivery_slot_calender', NULL)
            ->joinLeft(['ds' => 'delivery_slots'], 'ds.id_delivery_slot = dss.fk_delivery_slot', NULL)
            ->join(['sopm' => 'sales_order_pickup_method'], 'sopm.id_sales_order_pickup_method = sos.fk_sales_order_pickup_method', NULL)
            ->join(['sm' => 'store_master'], 'sm.id_store = so.store_id', NULL)
            ->join(['sois' => 'sales_order_item_status'], 'sois.id_sales_order_item_status = sos.status', NULL);

        $select->order('so.'. DbTable_Sales_OrderRow::CREATED_AT.' DESC');
        $select->limit($pageSize, $pageNo); //   LIMIT 20 OFFSET 10
        $result = $adapter->fetchAll($select);

        $allResult = new Bob_Db_Table_LimitedQueryWithTotalCount($select, null, null);
        $nextCount = intval($allResult->getCount()) - ($pageNo + $pageSize);
        if(intval($nextCount) < 0){
            $nextCount = 0;
        }
        return ['orders'=> $result, 'count'=> intval($allResult->getCount()), 'nextCount'=>$nextCount];

    }

    /**
     * @author Thejas N <thejas.nagabusan@embitel.com>
     * @task PKD-6241 - Order Search
     * @param type $orderNumber string mandatory
     * @param type $pageNo int mandatory
     * @param type $pageSize int mandatory
     */
    public function searchOrderByOrderNumber($orderNumber, $pageNo, $pageSize, $bbstoreCode = null) {
        $adapter = Zend_Db_Table::getDefaultAdapter();
        $select = $adapter->select();

        $columns = $this->_orderSearchColumns();

        $SOS = $adapter
                    ->select()
                    ->from('sales_order_shipment')
                    ->where('order_nr = ?', (string)$orderNumber);

        $select->from(['sos' => $SOS], $columns);

        if($bbstoreCode){
            $select->join(['so' => 'sales_order'], 'so.id_sales_order = sos.fk_sales_order AND sos.store_code ='.$bbstoreCode, NULL);
        }else{
            $select->join(['so' => 'sales_order'], 'so.id_sales_order = sos.fk_sales_order',NULL);
        }

        $select->joinLeft(['dss' => 'delivery_store_slots'], 'sos.fk_delivery_store_slot = dss.id_delivery_store_slot', NULL)
            ->joinLeft(['dsc' => 'delivery_slots_calender'], 'dsc.id_delivery_slot_calender = dss.fk_delivery_slot_calender', NULL)
            ->joinLeft(['ds' => 'delivery_slots'], 'ds.id_delivery_slot = dss.fk_delivery_slot', NULL)
            ->join(['sopm' => 'sales_order_pickup_method'], 'sopm.id_sales_order_pickup_method = sos.fk_sales_order_pickup_method', NULL)
            ->join(['sm' => 'store_master'], 'sm.id_store = so.store_id', NULL)
            ->join(['sois' => 'sales_order_item_status'], 'sois.id_sales_order_item_status = sos.status', NULL)
            ->join(['ccn' => 'customer_contact_number'], 'ccn.fk_customer = so.fk_customer', NULL);


        $select->order('so.'. DbTable_Sales_OrderRow::CREATED_AT.' DESC');
        $select->limit($pageSize, $pageNo); //   LIMIT 20 OFFSET 10
        $result = $adapter->fetchAll($select);

        $allResult = new Bob_Db_Table_LimitedQueryWithTotalCount($select, null, null);
        $nextCount = intval($allResult->getCount()) - ($pageNo + $pageSize);
        if(intval($nextCount) < 0){
            $nextCount = 0;
        }
        return ['orders'=> $result, 'count'=> intval($allResult->getCount()), 'nextCount'=>$nextCount];
    }

    /**
     * @task PKD-8217 Cash Declared shipments API
     * @author Saroj Singh <sarojkumar.singh@tathastu.ai>
     * @param type $data['storeCode'] string mandatory
     * @param type $data['isInstorePickup'] string mandatory
     */
    public function getCashDeclaredShipments($data = []) {

            $storeCode = $data['storeCode'];
            $isInstorePickup = $data['isInstorePickup'];

            $dateTime = new DateTime();
            //Set the date to -7 days using the modify function.
            $dateTime->modify('-7 day');
            //date in a YYYY-MM-DD format.
            $from_date =  $dateTime->format("Y-m-d");

            $timeInput = strtotime('10/17/2019');
            $dateInput = date('Y-m-d',$timeInput);

            $db = Zend_Db_Table_Abstract::getDefaultAdapter();
            $resp = array();
            if($isInstorePickup){
                //fetch delivered shipments list for In-store pickup and Extended store In-Store Pickup

                $shipmentMethod = array(1,6);

                $select = $db->select();

                $select->from(array('sm' => 'store_master'), array(''))
                    ->joinInner(array('so' => 'sales_order'), 'sm.id_store = so.store_id', array('order_nr'))
                    ->joinInner(array('sos' => 'sales_order_shipment'),'so.order_nr = sos.order_nr', array('shipment_id', 'shipment_grandtotal', 'updated_at'))
                    ->joinInner(array('sois' => 'sales_order_item_status'), 'sois.id_sales_order_item_status = sos.status', array(''))
                    ->joinInner(array('si' => 'sales_invoice'), 'sos.shipment_id = si.shipment_id', array('order_data'))
                    ->where('sois.name = ?', 'delivered')
                    ->where('sos.fk_sales_order_pickup_method IN (?)', $shipmentMethod)
                    ->where('sm.store_code=?', (string) $storeCode)
                    ->where('so.payment_method =?', 'CashOnDelivery')
                    ->where('si.created_at >= ?', $from_date)
                    ->where('si.created_at > ?', $dateInput);


                $resultSet = $db->fetchAll($select);
                $shipments = [] ;
                $totalAmount = [] ;
                foreach ($resultSet as $result) {
                    $item['shipmentId'] = $result['shipment_id'];
                    $item['orderId'] = $result['order_nr'];
                    $item['Amount'] = $result['shipment_grandtotal'];
                    $item['deliveredDateTime'] = $result['updated_at'];
                    $orderData = (json_decode($result['order_data'], TRUE));
                    if ( ! isset($totalAmount[$orderData['order']['invoice']['tillNumber']])) {
                        $totalAmount[$orderData['order']['invoice']['tillNumber']] = $result['shipment_grandtotal'];
                     }else {
                      $totalAmount[$orderData['order']['invoice']['tillNumber']] = $totalAmount[$orderData['order']['invoice']['tillNumber']] + $result['shipment_grandtotal'];
                     }
                    $shipments[$orderData['order']['invoice']['tillNumber']][] =  $item;
                }
                $select = $db->select();
                $select->from(array('tripTable' => 'trip'), array('balance','till_or_mobile'))
                        ->where('tripTable.id_trip IN(?)',$db->select()->from(array('tripTable' => 'trip'), array('max(id_trip)'))
                        ->where('tripTable.vendor_type = ?', 'picker')
                        ->where('tripTable.store_code = ?', $storeCode)
                        ->order('tripTable.declared_datetime DESC')
                        ->group('tripTable.till_or_mobile'));

                $balResultSet = $db->fetchAll($select);

                foreach($balResultSet as $result){
                   $tillNo = $result['till_or_mobile'];
                   $respItem = array();
                   $respItem['tillNo'] = $tillNo;
                   $respItem['balance']  = $result['balance'];
                   if(isset($shipments[$tillNo])){
                      $respItem['shipments'] = $shipments[$tillNo];
                      $respItem['totalAmount'] = $totalAmount[$tillNo] + $respItem['balance'];
                      $resp[] = $respItem;
                      unset($shipments[$tillNo]);
                   }else{
                       if($respItem['balance'] != 0){
                           $respItem['totalAmount'] = $respItem['balance'];
                           $resp[] = $respItem;
                       }
                   }
                }

                $tillNos = array_keys($shipments);
                foreach($tillNos as $tillNo){
                    $respItem = array();
                    $respItem['tillNo'] = $tillNo;
                    $respItem['shipments'] = $shipments[$tillNo];
                    $respItem['balance'] = 0;
                    $respItem['totalAmount'] = $totalAmount[$tillNo];
                    $resp[] = $respItem;
                }
                return $resp;

            }else {
                //fetch delivered shipments list for Standard-devlivery and Extended store delivery
                $select = $db->select();
                $select->from(array('sm' => 'store_master'), array('vendor_name', 'id_store'))
                        ->where('sm.store_code = ?', $storeCode);

                 $vendor = $db->fetchAll($select);
                 $vendorName = null;
                 $storeId = null;
                 if(!empty($vendor)){
                     $vendorName = $vendor[0]['vendor_name'];
                     $storeId = $vendor[0]['id_store'];
                  }

                $shipmentMethod = array(2,5);

                $select = $db->select();

                $select->from(array('so' => 'sales_order'),  array('order_nr'))
                    ->joinInner(array('sos' => 'sales_order_shipment'),'sos.order_nr = so.order_nr', array('shipment_id','shipment_grandtotal'))
                    ->joinInner(array('sois' => 'sales_order_item_status'), 'sois.id_sales_order_item_status = sos.status', array(''))
                    ->joinInner(array('soid' => 'sales_order_item_delivery'), 'soid.shipment_id = sos.shipment_id', array('delivery_boy_phone','actual_delivery_date','created_at' => 'max(soid.created_at)', 'id_sales_order_item_delivery' => 'max(soid.id_sales_order_item_delivery)'))
                    ->where('so.store_id =?', intVal($storeId))
                    ->where('so.payment_method =?','CashOnDelivery')
                    ->where('soid.created_at >= ?',$from_date)
                    ->where('soid.created_at > ?', $dateInput)
                    ->where('sois.name = ?', 'delivered')
                    ->where('sos.fk_sales_order_pickup_method IN (?)', $shipmentMethod)
                    ->group('sos.shipment_id');


                $resultSet = $db->fetchAll($select);
                $shipments = [] ;
                $totalAmount = [] ;
                foreach ($resultSet as $result) {
                    $item['shipmentId'] = $result['shipment_id'];
                    $item['orderId'] = $result['order_nr'];
                    $item['Amount'] = $result['shipment_grandtotal'];
                    $item['deliveredDateTime'] = empty($result['actual_delivery_date'])?$result['created_at']:$result['actual_delivery_date'];
                    if ( ! isset($totalAmount[$result['delivery_boy_phone']])) {
                        $totalAmount[$result['delivery_boy_phone']] =  $result['shipment_grandtotal'];
                       } else {
                         $totalAmount[$result['delivery_boy_phone']] = $totalAmount[$result['delivery_boy_phone']] + $result['shipment_grandtotal'];

                       }
                    $shipments[$result['delivery_boy_phone']][] =  $item;
                }

                $select = $db->select();
                $select->from(array('tripTable' => 'trip'), array('balance','till_or_mobile'))
                        ->where('tripTable.id_trip IN(?)',$db->select()->from(array('tripTable' => 'trip'), array('max(id_trip)'))
                        ->where('tripTable.vendor_type = ?', 'field_executive')
                        ->where('tripTable.store_code = ?', $storeCode)
                        ->order('tripTable.declared_datetime DESC')
                        ->group('tripTable.till_or_mobile'));

                $balResultSet = $db->fetchAll($select);

                foreach($balResultSet as $result){
                   $mobileNo = $result['till_or_mobile'];
                   $respItem = array();
                   $respItem['fieldAgentMobile'] = $mobileNo;
                   $respItem['companyName'] = $vendorName;
                   $respItem['balance']  = $result['balance'];
                   if(isset($shipments[$mobileNo])){
                      $respItem['shipments'] = $shipments[$mobileNo];
                      $respItem['totalAmount'] = $totalAmount[$mobileNo] + $respItem['balance'];
                      $resp[] = $respItem;
                      unset($shipments[$mobileNo]);
                   }else {
                       if($respItem['balance'] != 0){
                           $respItem['totalAmount'] = $respItem['balance'];
                           $resp[] = $respItem;
                       }
                   }
                }
                $mobileNos = array_keys($shipments);
                foreach($mobileNos as $mobileNo){
                    $respItem = array();
                    $respItem['fieldAgentMobile'] = $mobileNo;
                    $respItem['shipments'] = $shipments[$mobileNo];
                    $respItem['companyName'] = $vendorName;
                    $respItem['balance'] = 0;
                    $respItem['totalAmount'] = $totalAmount[$mobileNo];
                    $resp[] = $respItem;
                }
                return $resp;
            }
    }


    private function _orderSearchColumns() {
        return [
            'customerId' =>'so.'. DbTable_Sales_OrderRow::FK_CUSTOMER,
            'customerName' => new Zend_Db_Expr("CONCAT(".DbTable_Sales_OrderRow::CUSTOMER_FIRST_NAME." ,' ', ".DbTable_Sales_OrderRow::CUSTOMER_LAST_NAME.")"),
            'orderId' =>'so.'. DbTable_Sales_OrderRow::ID_SALES_ORDER,
            'shipmentGrandTotal' =>'sos.'. DbTable_Sales_Order_ShipmentRow::SHIPMENT_GRANDTOTAL,
            'orderNumber' => 'sos.'. DbTable_Sales_Order_ShipmentRow::ORDER_NR,
            'orderDate' =>'so.'. DbTable_Sales_OrderRow::CREATED_AT,
            'shipmentId' =>'sos.'. DbTable_Sales_Order_ShipmentRow::SHIPMENT_ID,
            'deliveryDate' =>'dsc.'. DbTable_Delivery_Slots_CalenderRow::DATE,
            'startTime' =>'ds.'. DbTable_Delivery_SlotsRow::START_TIME,
            'endTime' =>'ds.'. DbTable_Delivery_SlotsRow::END_TIME,
            'pickupMethod' =>'sopm.'. DbTable_Sales_Order_Pickup_MethodRow::PICKUP_METHOD,
            'paymentMethod' =>'so.'. DbTable_Sales_OrderRow::PAYMENT_METHOD,
            'storeName' =>'sm.'. DbTable_Store_MasterRow::STORE_NAME,
            'storeCode' =>'sos.'. DbTable_Sales_Order_ShipmentRow::STORE_CODE,
//            'storeCode' =>'sm.'. DbTable_Store_MasterRow::STORE_CODE,
            'storeId' =>'sm.'. DbTable_Store_MasterRow::ID_STORE,
            'baseStoreCode' =>'sm.'. DbTable_Store_MasterRow::STORE_CODE,
            'contactNumber' =>'ccn.'. DbTable_Customer_Contact_NumberRow::CONTACT_NUMBER,
            'status' => 'sois.'.DbTable_Sales_Order_Item_StatusRow::NAME,
        ];
    }

    /**
     * @author Thejas N <thejas.nagabusan@embitel.com>
     * @task PKD-8135 | PKD-8195
     * @param type $states array of states mandatory
     */
    public function getStatesCodeByName($states){
        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()
                ->from(array('sois' => 'sales_order_item_status'),
                    array('id_sales_order_item_status'))
                ->where("name IN (?)",$states);
        $result =  $select->query()->fetchAll();

        if(!empty($result)){
            $statesCode = [];
            foreach ($result as $key => $value) {
                $statesCode[]=$value['id_sales_order_item_status'];
            }
            return $statesCode;
        }

        return false;
    }

    public function getExShipmentAck($data = []) {
        try{
            $logFileName = 'getExShipmentAck_'.date('Y-m-d').'.log';
            $itemsFound = false;
            $itemMissed = [];
            $itemList = [];
            $shipmentArray= [];
            $inputItemsArray = [];
            $inwardedItemsList = [];
            $failedItemsList= [];
            $allowedPickUpMethods = [2,3,5];
            $is_instorePickUpOrder = true;
            $result = [];
            $allInputItemIds=[];
            $checkAllInputItemIds =[];

            if (empty($data['shipmentId'])) {
                return ['responseCode' => 412, 'responseMessage' => 'Shipment ID is empty'];
            }else{
                $shipmentService = new OrderprocessingExt_Service_Shipment();
                $shipmentArray = $shipmentService->getModel()->getItemsByShipmentItems($data['shipmentId']);
            }

            //Invalid Shipment
            if (empty($shipmentArray)) {
                return ['responseCode' => 412, 'responseMessage' => 'Invalid Shipment Id'];
            }

            //Validate input array
            if(empty ($data['data']['items'])){
                return ['responseCode' => 412, 'responseMessage' => 'itemList should not be empty'];
            }else {
                $inputItemsArray = $data['data']['items'];
                foreach ($inputItemsArray as $items) {
                    if(!isset($items['itemId'])){
                        return ['responseCode' => 412, 'responseMessage' => 'Invalid Item array'];
                    }
                    $allInputItemIds[] = $items['itemId'];

                    if(isset($items['inward']) && $items['inward']){
                        $inwardedItemsList[] = $items;
                    }else if(isset($items['inward']) && !$items['inward']){
                        $failedItemsList[] = $items;
                    }else{
                        return ['responseCode' => 412, 'responseMessage' => 'Invalid Item array'];
                    }
                }
            }

            //get all the items which are in ed_inward state
            $allowed_states = $this->getStatesCodeByName(['ed_inward']);

            if(!empty($inwardedItemsList)){
                $itemList = array_column($inwardedItemsList,'itemId');
            }

            //Validate item lists
            foreach ($shipmentArray as $item) {
                if(in_array($item['fk_sales_order_item_status'], $allowed_states)){
                    $checkAllInputItemIds [] = $item['fk_sales_order_item'];
                }
            }

            if(array_diff($checkAllInputItemIds,$allInputItemIds)){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid Items list.'];
            }

            if(!empty($failedItemsList)){
                //CALL REMOVE ITEM API V2
                $removeItemdata = [];
                //Creating array to remove items
                $removeItemdata['shipmentId'] = $data['shipmentId'];
                $itemsToProcess = [];
                foreach ($failedItemsList as $items) {
                    $itemsToProcess[] = [
                                        'itemId' => $items['itemId'],
                                        'reason' => $items['comments'],
                                        'qty' =>1
                                            ];
                }
                $removeItemdata['itemsToProcess'] = $itemsToProcess;
                $removeItemdata['isDelivered'] = false;
                $removeItemdata['source'] = 'pickerApp';
                $orderprocessModel =  new OrderprocessingExt_Model_OrderModification();
                $failedItemResults =  $orderprocessModel->removeItemsByShipment($removeItemdata);
                Bob_Log::debug('Remove item data ' . var_export($removeItemdata, true), $logFileName);
                Bob_Log::debug('Remove result ' . var_export($failedItemResults, true), $logFileName);

                if(!empty($failedItemResults)){
                    if(empty($failedItemResults['removedItems'])){
                        return $failedItemResults;
                    }else{
                        foreach ($failedItemResults['removedItems'] as $items) {
                            foreach ($items['itemIds'] as $itemId) {
                                $result[$itemId] = 'Item Removed';
                            }
                        }
                        //Send shipmentId to queue to generate reject invoice
                        $queueConsumeService = new OrderprocessingExt_Service_QueueConsume();
                        $rejectInvoiceToQueue = $queueConsumeService->_getModel->sendRejectShipmentToQueue($data['shipmentId']);
                    }
                }
            }

            $order_service = new Erp_Service_Erp();
            //Getting $shipmentArray new to mark all items shipped from ed_inward
            $shipmentArray = $shipmentService->getModel()->getItemsByShipmentItems($data['shipmentId']);
            $itemsToFareye = [];
            foreach ($shipmentArray as $item) {

                if(in_array($item['fk_sales_order_pickup_method'],$allowedPickUpMethods)){
                    $is_instorePickUpOrder = false;
                }

                $data['order_nr'] = $item['order_nr'];
                if (in_array($item['fk_sales_order_item_status'], $allowed_states)) {
                    $orderUpdate = $order_service->setOrderStatus($item['order_nr'], $item['fk_sales_order_item'], Orderprocessing_Service_Interface_StatusConstant::STATUS_SHIPPED, date('Y-m-d'), date('H:i:s'), 'ExShipment ACK API');
                    $result[$item['fk_sales_order_item']] = $orderUpdate;
                    $itemsToFareye[] = $item['fk_sales_order_item'];
                    if ($orderUpdate) {
                        Bob_Log::debug($item['fk_sales_order_item'] . ' >> STATUS >>: ' . var_export($orderUpdate, true), $logFileName);
                    } else {
                        Bob_Log::debug($item['fk_sales_order_item'].' >> Failed to change the item state', $logFileName);
                    }
                }
            }

            if(!$is_instorePickUpOrder){
                //Check previous state has shipped
                if($this->_eligibleToDeliveryApp($itemsToFareye)){
                    //Send shipment to Fareye
                    $customerLat = '';
                    $customerLng = '';
                    $this->_getCustomerDefaultShippingAddressByOrderNr($data['order_nr'],$customerLat,$customerLng);
                    $itemService = new OrderprocessingExt_Model_Order_Item();

                    try{
                        $storeCode = $itemService->_getMappedStoreFromDB($data['shipmentId']);
                        $itemService->deliveryApiWhenStatusReadyToShip($data['order_nr'],$data['shipmentId'].'_1', $itemsToFareye, $storeCode['store_code'] ,$customerLat, $customerLng);
                    }catch(Exception $e){
                        Bob_Log::debug('Exception while ACK Shipment ' . var_export($e->getMessage(), true), $logFileName);
                        return ['responseCode' => 500, 'responseMessage' => 'Exception Sending data to Delivery app'];
                    }
                }
            }

            if(!empty($result)){
                return ['responseCode' => 200, 'responseMessage' => 'Shipment Successfully Shipped','items'=>$result];
            }
            return ['responseCode' => 412, 'responseMessage' => 'Please verify Items and its states'];
        }catch(Exception $e){
            $shipmentId = !empty($data['shipmentId']) ? $data['shipmentId'] : "NA";
            Bob_Log::debug('OrderShipment getExShipmentAck Shipment Id = '. $shipmentId. ' EXCEPTION :' .
            var_export($e->getMessage(), true), 'exception_ordermodification.log');
            Bob_Log::debug('Exception while ACK Shipment ' . var_export($e->getMessage(), true), $logFileName);
            return ['responseCode' => 500, 'responseMessage' => 'Exception while ACK Shipment'];
        }

    }

    protected function _getCustomerDefaultShippingAddressByOrderNr($orderNumber,&$customerLat,&$customerLng) {
        $adapter = Zend_Db_Table::getDefaultAdapter();
        $select = $adapter->select()
                ->from(array('so' => 'sales_order'),null)
                ->join(array('ccn'=>'customer_contact_number'), 'ccn.fk_customer = so.fk_customer',null)
                ->join(array('ca'=>'customer_address'), 'ca.phone = ccn.contact_number',array('latitude','longitude'))
                ->where("so.order_nr = '$orderNumber'")
                ->where("ca.is_default_shipping = 1");
        $result =  $adapter->fetchRow($select);

        if(!empty($result)){
            $customerLat = $result['latitude'];
            $customerLng = $result['longitude'];
        }
    }


    private function _getShipmentFormatStoreCode($shipmentId){
        $db = Zend_Db_Table_Abstract::getDefaultAdapter();
        $select = $db->select();
        $select->from(['SOS' => 'sales_order_shipment'], ['store_format'])
            ->joinLeft(['SM' => 'store_master'], 'SOS.store_code = SM.store_code', ['id_store'])
            ->where('SOS.shipment_id = ?', $shipmentId);
        $result = $db->fetchRow($select);
        return $result;
    }

    private function _eligibleToDeliveryApp($itemList) {
        if(empty($itemList)){
            return false;
        }
        $db = Zend_Db_Table_Abstract::getDefaultAdapter();
        $select = $db->select();
        $select->from(['sois' => 'sales_order_item_status_history'], '*')
            ->where('sois.fk_sales_order_item IN (?)', $itemList)
            ->where('sois.fk_sales_order_item_status = 5');
        $result = $db->fetchAll($select);
        if(!empty($result)){
            if(count($itemList) < count($result)){
                return false;
            }
            return true;
        }
        return false;
    }


// BBG-725
    public function getAttachmentModel()
    {
        $attachmentModel = new OrderprocessingExt_Model_Order_Attachments();
        return $attachmentModel;
    }

    public function changingTheQuantityForWeight($returns){
        foreach ($returns['HSN'] as $key) {
            foreach ($returns['items'] as $item => &$value) {
                $item_key = $item;
                $sku = explode("-",$value['sku']);
                if($key['sku'] == $sku[0]){
                    if(strtolower($key['uom']) == "weight" && $key['packSize']!=""){
                        $packsize = explode(" ",$key['packSize']);
                        $new_quanity = ($value['quantity']) * ($packsize[0]);
                        $value['qty'] = $new_quanity ." ".strtolower($packsize[1]);
                        $value['uom'] = strtoupper( substr( $key['uom'], 0, 2 ) );
                    }
                    else{
                        $value['qty'] = $value['quantity'] ." qty";
                        $value['uom'] = strtoupper( substr( $key['uom'], 0, 2 ) );
                    }
                }
            }
        }
        return $returns;
    }

     // BBG-725
    public function saveRejectInvoice($returns,$eodStoreCode,$lineItemIds,$invoiceType)
    {
        $order_details = json_encode($returns);
        $lineItemIdEncoded = json_encode($lineItemIds);
        $db = Zend_Db_Table::getDefaultAdapter();
        $fields = array( DbTable_Sales_InvoiceRow::INVOICE_NR => $returns['invoice_details']['invoiceNumber'],
                         DbTable_Sales_InvoiceRow::FK_SALES_ORDER => $returns['result'][0]['id_sales_order'],
                         DbTable_Sales_InvoiceRow::ORDER_NR => $returns['result'][0]['order_nr'],
                         DbTable_Sales_InvoiceRow::STORE_CODE => $returns['store']['store_code'],
                         DbTable_Sales_InvoiceRow::EOD_STORE_CODE => $eodStoreCode,
                         DbTable_Sales_InvoiceRow::SHIPMENT_ID => $returns['shipment_id'],
                         DbTable_Sales_InvoiceRow::SHIPMENT_GRAND_TOTAL => $returns['shipment_grandtotal'],
                         DbTable_Sales_InvoiceRow::INVOICE_BARCODE => '',
                         DbTable_Sales_InvoiceRow::ORDER_DATA => $order_details,
                         DbTable_Sales_InvoiceRow::LINE_ITEM_IDS => $lineItemIdEncoded,
                         DbTable_Sales_InvoiceRow::DUPLICATE_CHECK => 1,
                         DbTable_Sales_InvoiceRow::CREATED_AT => date("Y-m-d H:i:s"),
                         DbTable_Sales_InvoiceRow:: INVOICE_TYPE => $invoiceType
                    );


        $existsArr = $this->getOrderIdIfExistsForReject($returns['result'][0]['id_sales_order'], $returns['shipment_id'],$invoiceType);

        if(isset($existsArr['cnt']) && $existsArr['cnt'] > 0){
            if($existsArr['duplicate_check'] == 1){
                $this->duplicateCheckUpdate($fields['fk_sales_order'],$fields['shipment_id']);
            }
            return false;
        }else{
            $table = new DbTable_Sales_InvoiceTable();
            $result =  $db->insert($table->getName(), $fields);
            return true;
        }
    }

    // BBG-725
    public function getOrderIdIfExistsForReject($orderId, $shipmentId, $invoiceType){
        $db = Zend_Db_Table::getDefaultAdapter();

        $result = $db->fetchRow("SELECT COUNT(*) as cnt,invoice_nr,duplicate_check FROM sales_invoice WHERE fk_sales_order = $orderId  AND shipment_id = '$shipmentId' AND invoice_type  in ('$invoiceType') group by id_sales_invoice");
        return (!empty($result)) ? $result : '';
    }

    // BBG-725
    public function duplicateCheckUpdate($param,$return_request_nr) {
        $result = array();
        $result['updated_at'] = date("Y-m-d H:i:s");
        $dataArray[DbTable_Sales_InvoiceRow::DUPLICATE_CHECK] = 2;
        $dataArray[DbTable_Sales_InvoiceRow::UPDATED_AT] = date("Y-m-d H:i:s");
        $dbTableObject = new DbTable_Sales_InvoiceTable();
        $where = array();
        $where[] = 'fk_sales_order = ' . $param;
        $where[] = "shipment_id = '.$return_request_nr.'";
        $where[] = "invoice_type = 'return' ";
        $dbTableObject->update($dataArray,$where);
        return $result;
    }

    public function getInvoiceNumber($strCode)
    {
        $salesInvoice = new DbTable_Sales_InvoiceTable();
        $select = $salesInvoice->getAdapter()->select();
        $select->from($salesInvoice->getName(), DbTable_Sales_InvoiceRow::INVOICE_NR)
                ->where(DbTable_Sales_InvoiceRow::STORE_CODE." = '".$strCode."'")
                ->order(DbTable_Sales_InvoiceRow::ID_SALES_INVOICE .' DESC')
                ->limit(1);

        $result = $salesInvoice->getAdapter()->fetchRow($select);
        return (!empty($result)) ? $result : '';
    }
    /*
     * PKD-7551 Get Shipment ID and store format using the Order Item ID
     *
     * @param  int $orderItemId id_sales_order_item
     * @return Array()
     */
    public function getShipmentIdFormatByOrderItemId($orderItemId) {
        $adapter = Zend_Db_Table::getDefaultAdapter();

        $select = $adapter->select();
        $select->from(['ss' => 'sales_order_shipment'], ['shipment_id' => 'ss.shipment_id', 'shipment_format' => 'ss.store_format']);
        $select->join(['si' => 'sales_order_shipment_item'], 'si.fk_sales_order_shipment = ss.id_sales_order_shipment',[]);
        $select->where('si.fk_sales_order_item = ?', $orderItemId);
        $shipmentRow = $adapter->fetchRow($select);
        return ( ! empty($shipmentRow['shipment_id'])) ? $shipmentRow : false;
    }
    private function _isRoundUp($fkShippingRegion){

        $orderProcConfig= new ConfigurationExt_Service_Loader_Namespace('orderprocessing-ext', true);
        $statesString = $orderProcConfig->getValue('rounding_exclusion_states');
        //Making states case insensitive to search below
        $statesToExclude = isset($statesString)? explode(",",strtolower($statesString)) : array();
        $adapter          = (new Api_Service_Service())->getStorageAdapter();
        $states  = $adapter->get('regions', false, false);
        $stateName = '';
        foreach($states as $state){
            if($state['id_customer_address_region'] == $fkShippingRegion){
                $stateName = $state['name'];
            }
        }
        if(!in_array(strtolower($stateName),$statesToExclude)) {
            return true;
        }
        else{
            return false;
        }
    }


    public function getShipmentList($baseStoreCode, $status = null, $shipmentType = null, $pageNo = null, $perPage = null, $fromDate = null, $toDate = null){
        if(empty($fromDate) || empty($toDate)){
            $fromDate = date("Y-m-d") . " 00:00:00";
            $toDate = date("Y-m-d") . " 23:59:59";
        } else {
            $fromDate = $fromDate . " 00:00:00";
            $toDate = $toDate . " 23:59:59";
        }

        if(empty($pageNo) || empty($perPage)) {
            $pageNo = 1;
            $perPage = 10;
        }

        if(!empty($status)){
            $status = strtolower($status);
        }

        $db = Zend_Db_Table::getDefaultAdapter();
        $select = $db->select()->from(array('store_master'), array("id_store"))->where("store_code = '$baseStoreCode'");
        $storeId = $select->query()->fetch();
        $storeMappingService = new Storemaster_Service_StoreMapping();
        $storeMappingModel = $storeMappingService->getStoreMappingModel();
        $store = $storeMappingModel->getAllMappedStoreIdsWithStore($baseStoreCode);
        $store = array_values($store);
        if(!empty($storeId['id_store'])) {
            $store[] = $storeId['id_store'];
        }

        $select = $db->select()->from(array('sos' => 'sales_order_shipment'), array("shipment_id", "order_nr", "store_format", "created_at"))
            ->joinLeft(array('dss'=>'delivery_store_slots'),'dss.id_delivery_store_slot = sos.fk_delivery_store_slot',array())
            ->joinLeft(array('dsc'=>'delivery_slots_calender'),'dsc.id_delivery_slot_calender = dss.fk_delivery_slot_calender',array("date"))
            ->joinLeft(array('ds'=>'delivery_slots'),'ds.id_delivery_slot = dss.fk_delivery_slot',array("start_time","end_time"))
            ->joinLeft(array('so'=>'sales_order'),'sos.fk_sales_order = so.id_sales_order',array())
            ->joinLeft(array('sm'=>'store_master'),'so.store_id = sm.id_store',array("sm.store_code"))
            ->joinLeft(array('sois'=>'sales_order_item_status'),'sos.status = sois.id_sales_order_item_status',array("status_name" => "sois.name"))
            ->where("dsc.date >= '$fromDate'")
            ->where("dsc.date <= '$toDate'")
            ->where("sois.name = '$status'");
        if(!empty($shipmentType)) {
            if (strtolower($shipmentType) == "standard") {
                $select->where("sos.store_format != 'ED'");
            } else if (strtolower($shipmentType) == "express") {
                $select->where("sos.store_format = 'ED'");
            }
        }
        $select->where('so.store_id in (?)', $store);
        $select->limit($perPage, (($pageNo - 1) * $perPage));

        $response = $select->query()->fetchAll();
        $final = array();
        foreach ($response as $resp) {
            $finalizedResponse = array();
            $finalizedResponse['shipmentId'] = $resp['shipment_id'];
            $finalizedResponse['orderNr'] = $resp['order_nr'];
            $finalizedResponse['shipmentType'] = ($resp['store_format'] == "ED") ? "express" : "standard";
            $finalizedResponse['storeCode'] = $resp['store_code'];
            $finalizedResponse['createdAt'] = $resp['created_at'];
            $finalizedResponse['scheduledDeliveryDate'] = $resp['date'];
            $finalizedResponse['scheduledDeliveryStartTime'] = $resp['start_time'];
            $finalizedResponse['scheduledDeliveryEndTime'] = $resp['end_time'];
            $finalizedResponse['status'] = $resp['status_name'];
            $final[] = $finalizedResponse;
        }
        return $final;
    }


    /**
     * SKA-38 - Order Search API for Store APP
     * @author Shankar Modi
     * @param type $data
     * @return type
     */
    public function storeOrdersSearch($data = []) {
        try {
            if(empty($data['search_by'])){
                return ['responseCode' => 412, 'responseMessage' => 'Search Type should not be empty'];
            }

            if(empty($data['search_term'])){
                return ['responseCode' => 412, 'responseMessage' => 'Search value should not be empty'];
            }

            //Validating Search Type
            if(strtolower($data['search_by']) != 'order_number' && strtolower($data['search_by']) != 'phone_number'){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid search type : ' . $data['search_by']];
            }

            //Validating Search value
            if(!is_numeric($data['search_term'])){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid search value : ' . $data['search_term']];
            }else if(strtolower ($data['search_by']) == 'phone_number' && strlen($data['search_term']) != 10){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid Phone number : ' . $data['search_term']];
            }
            if((isset($data['offset']) && !is_numeric($data['offset'])) || (isset($data['limit']) && !is_numeric($data['limit']))){
                return ['responseCode' => 412, 'responseMessage' => 'Invalid Offset or Limit'];
            }

            $offset = isset($data['offset']) ? intval($data['offset']): 0;
            $limit = isset($data['limit']) ? intval($data['limit']) : 10;

            if($limit < 0){
                return ['responseCode' => 412, 'responseMessage' => 'Limit Should be greater than 0'];
            }
            if($offset < 0){
                return ['responseCode' => 412, 'responseMessage' => 'Offset Should be greater than 0'];
            }

            $order =   $this->_findOrder($data['search_by'], $data['search_term'], $limit, $offset);
            return $order;

        } catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug('storeOrdersSearch()  EXCEPTION :' . var_export($exceptionMsg, true), 'storeOrderSearch.log');
            return ['responseCode' => 500, 'responseMessage' => $exceptionMsg];
        }
    }


    /**
     * SKA-38
     * @author Shankar Modi
     * @param type $searchTerm
     * @param type $searchVal
     * @param type $limit
     * @param type $offset
     * @return type
     */
    private function _findOrder($searchTerm, $searchVal,$limit=10, $offset=0){
        try {
            $adapter = Zend_Db_Table::getDefaultAdapter();
            $select = $adapter->select();
            $salesOrderFields   = array(
                'order_number'   => DbTable_Sales_OrderRow::ORDER_NR,
                'order_time'     => 'DATE_FORMAT(SO.'.DbTable_Sales_OrderRow::CREATED_AT.',"%Y-%m-%d %H:%i:%s")',
                'ordered_amount'   => DbTable_Sales_OrderRow::PAYABLE_AMOUNT,
                'payment_method' => DbTable_Sales_OrderRow::PAYMENT_METHOD,
                'status_id' => DbTable_Sales_OrderRow::FK_SALES_ORDER_ITEM_STATUS
            );
            $salesOrderItemFields=array(
                'items_ordered' => new Zend_Db_Expr('COUNT('.DbTable_Sales_Order_ItemRow::SKU.')'),
                'items_invoiced'=>new Zend_Db_Expr('sum(case when SI.fk_sales_order_item_status not in (1,2,3,4,9,13) then 1 else 0 end)'),

            );

            $salesOrderItemStatusFields=array(
                'status' => DbTable_Sales_Order_Item_StatusRow::EN_DISPLAY_NAME,
                'status_name' => DbTable_Sales_Order_Item_StatusRow::NAME,
            );

            $customerFields=array(
                'name' => new Zend_Db_Expr("CONCAT(first_name, ' ', last_name)"),

            );

            $customerContactFields=array(
                'phone' => 'contact_number',

            );

            $storeMasterFields=array(
                'store_code' => 'store_code',

            );

            $query  = $select->from(array('SO' => 'sales_order'),$salesOrderFields)
                             ->joinLeft(array('SI' =>'sales_order_item'), 'SO.'.DbTable_Sales_OrderRow::ID_SALES_ORDER.' = SI.'.DbTable_Sales_Order_ItemRow::FK_SALES_ORDER,$salesOrderItemFields)
                             ->joinLeft(array('SIS' =>'sales_order_item_status'), 'SO.'.DbTable_Sales_OrderRow::FK_SALES_ORDER_ITEM_STATUS .' = SIS.'.DbTable_Sales_Order_Item_StatusRow::ID_SALES_ORDER_ITEM_STATUS,$salesOrderItemStatusFields)
                             ->joinLeft(array('CUS' =>'customer'), 'CUS.'.DbTable_CustomerRow::ID_CUSTOMER.' = SO.'.DbTable_Sales_OrderRow::FK_CUSTOMER,$customerFields)
                             ->joinLeft(array('CCN' =>'customer_contact_number'), 'CUS.'.DbTable_CustomerRow::ID_CUSTOMER.' = CCN.'.DbTable_Customer_Contact_NumberRow::FK_CUSTOMER,$customerContactFields)
                             ->joinLeft(array('SM' =>'store_master'), 'SO.'.DbTable_Sales_OrderRow::STORE_ID.' = SM.'.DbTable_Store_MasterRow::ID_STORE,$storeMasterFields)
                             ->order('SO.'.DbTable_Sales_OrderRow::CREATED_AT . ' DESC')
                             ->group('SI.'.DbTable_Sales_Order_ItemRow::FK_SALES_ORDER)
                             ->limit($limit, $offset);

            switch ($searchTerm){
                case "order_number" :
                    $query->where('SO.'.DbTable_Sales_OrderRow::ORDER_NR . ' = ?', (string)$searchVal);
                    break;
                case "phone_number" :
                    $query->where('CCN.'.DbTable_Customer_Contact_NumberRow::CONTACT_NUMBER . ' = ?', $searchVal);
                    break;
            }

            $dbresultData   = $adapter->fetchAll($query);
            $allResult = new Bob_Db_Table_LimitedQueryWithTotalCount($select, null, null);
            $total = intval($allResult->getCount());

            //Get Order Status Codes from Configuration
            $service = new ConfigurationExt_Service_Configuration();
            $configExt = $service->getConfigurationConfig();
            $colorCodesStr = $configExt->order_status_color_codes;

            $codes = !empty($colorCodesStr) ? json_decode($colorCodesStr, true):[];

            foreach ($dbresultData as &$order) {
                $contactDetails = [];

                $order['items_ordered'] = intval($order['items_ordered']);
                $order['items_invoiced'] = intval($order['items_invoiced']);
                $order['ordered_amount'] = floatval($order['ordered_amount']);
                $contactDetails['name'] = $order['name'];
                $contactDetails['phone'] = $order['phone'];
                $order['customer_details'] = $contactDetails;
                unset($order['name']);
                unset($order['phone']);

                $order['status_colour_code'] = isset($codes[$order['status_id']]) ? $codes[$order['status_id']] : "";
                unset($order['status_id']);
                $shipment = $this->_shipmentDetailsByOrderNumber($order['order_number'], $order['store_code'], $order['payment_method'], $order['status_name']);
                $order['shipments'] = $shipment;
            }
            $response = [
                "search_by"=>$searchTerm,
                "result_entity"=> "order",
                "search_term"=> $searchVal,
                "error"=>  [],
                "meta" => [
                    "limit" => intval($limit),
                    "offset" => intval($offset),
                    "total" => $total,
                ],
                "results"=> $dbresultData
            ];
            return $response;
        }catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug('storeOrdersSearch()  EXCEPTION :' . var_export($exceptionMsg, true), 'storeOrderSearch.log');
            throw $ex;
        }
    }


    /**
     * SKA-38 - Get Shipment Details By Order Number
     * @author Shankar Modi
     * @param type $orderNumber
     * @param type $baseStoreCode
     * @return type
     */
    private function _shipmentDetailsByOrderNumber($orderNumber, $baseStoreCode=null, $paymentMethod=null, $orderStatus=null){
        try{
            $adapter = Zend_Db_Table::getDefaultAdapter();
            $select = $adapter->select();

            $shipmentcolumns = [
                "id"=> DbTable_Sales_Order_ShipmentRow::SHIPMENT_ID,
                "store_code" => DbTable_Sales_Order_ShipmentRow::STORE_CODE,
                "type" => DbTable_Sales_Order_ShipmentRow::STORE_FORMAT,
                "shipment_status" => DbTable_Sales_Order_ShipmentRow::STATUS,
            ];

            $deliveryDateColumns = [
                "delivery_date" => DbTable_Delivery_Slots_CalenderRow::DATE,
            ];

            $deliverySlotColumns = [
                "delivery_slot" => new Zend_Db_Expr("CONCAT(start_time, ' to ',end_time)"),
            ];

            $pickupMethodColumns = [
                "delivery_type" => DbTable_Sales_Order_Pickup_MethodRow::PICKUP_METHOD
            ];
            $SOS = $adapter
                        ->select()
                        ->from('sales_order_shipment')
                        ->where('order_nr = ?', (string)$orderNumber);

            $select->from(['sos' => $SOS], $shipmentcolumns);

            $select->joinLeft(['dss' => 'delivery_store_slots'], 'sos.fk_delivery_store_slot = dss.id_delivery_store_slot', NULL)
                ->joinLeft(['dsc' => 'delivery_slots_calender'], 'dsc.id_delivery_slot_calender = dss.fk_delivery_slot_calender', $deliveryDateColumns)
                ->joinLeft(['ds' => 'delivery_slots'], 'ds.id_delivery_slot = dss.fk_delivery_slot', $deliverySlotColumns)
                ->joinLeft(['sopm' => 'sales_order_pickup_method'], 'sos.fk_sales_order_pickup_method = sopm.id_sales_order_pickup_method', $pickupMethodColumns);

            $result = $adapter->fetchAll($select);

           foreach ($result as &$value) {
            $value['base_store_code'] =  $baseStoreCode;
            //Is eligible for return
            $bbstoreCode = $value['type'] != "ED" ? true : false;
            if($paymentMethod == 'CashOnDelivery'){
                $value['eligible_for_return'] = ($orderStatus == 'cash_declared' && !$bbstoreCode) ? true : false;

            }else{
                $value['eligible_for_return'] = ($orderStatus == 'delivered' && !$bbstoreCode) ? true : false;

            }

            //Is verifiable
            $value['is_verifiable'] = false;
            if($value['delivery_type'] == 'In-Store Pick-up' && $value['shipment_status'] == 5){
                $value['is_verifiable'] = true;
            }
             unset($value['shipment_status']);
           }

            return $result;
        }catch (Exception $ex) {
            $exceptionMsg = $ex->getMessage() . " FILE : " . $ex->getFile() . " LINE NO. : " . $ex->getLine();
            Bob_Log::debug('storeOrdersSearch()  EXCEPTION :' . var_export($exceptionMsg, true), 'storeOrderSearch.log');
            throw $ex;
        }
    }


    public function getShipmentIdsByOrderNr($orderNr) {
        $adapter = Zend_Db_Table::getDefaultAdapter();
        $select = $adapter->select();
        $select->from(['sos' => 'sales_order_shipment'], ['shipment_id' => 'sos.shipment_id']);
        $select->joinLeft(['so' => 'sales_order'], 'sos.fk_sales_order = so.id_sales_order',[]);
        $select->where('so.order_nr = ?', (string)$orderNr);
        $shipmentIds = $adapter->fetchAll($select);
        return $shipmentIds;

    }
}
