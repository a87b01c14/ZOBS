CLASS zcl_obs DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    TYPES:
      BEGIN OF ty_signature,
        method    TYPE string,
        date      TYPE string,
        filename  TYPE rlgrap-filename,
        signature TYPE string,
      END OF ty_signature .
    TYPES:
      BEGIN OF ty_http_return,
        msg           TYPE string,
        status_code   TYPE i,
        status_reason TYPE string,
        response      TYPE string,
        responsex     TYPE xstring,
      END OF ty_http_return .

    CLASS-DATA gs_conf TYPE ztobs_conf .

    METHODS constructor
      IMPORTING
        !iv_bucket TYPE ztobs_conf-zbucket .
    CLASS-METHODS class_constructor .
    CLASS-METHODS remove_file_from_obs
      IMPORTING
        !iv_filename     TYPE rlgrap-filename
      RETURNING
        VALUE(rs_return) TYPE bapiret2 .
    CLASS-METHODS upload_file_to_obs
      RETURNING
        VALUE(rs_return) TYPE zcl_common=>ty_upload_server_return .
    CLASS-METHODS read_file_from_obs
      IMPORTING
        !iv_filename     TYPE rlgrap-filename
      RETURNING
        VALUE(rs_return) TYPE zcl_common=>ty_read_file_return .
    CLASS-METHODS download_file_from_obs
      IMPORTING
        !iv_filename     TYPE rlgrap-filename
      RETURNING
        VALUE(rs_return) TYPE bapiret2 .
    CLASS-METHODS show_picture_from_obs
      IMPORTING
        !iv_filename TYPE rlgrap-filename
        !io_picture  TYPE REF TO cl_gui_picture OPTIONAL .
    CLASS-METHODS convert_gmt_date
      IMPORTING
        VALUE(iv_timestamp) TYPE timestamp OPTIONAL
      RETURNING
        VALUE(rv_date)      TYPE string .
    CLASS-METHODS get_signature
      CHANGING
        VALUE(cs_signature) TYPE ty_signature .
    CLASS-METHODS call_obs_api
      IMPORTING
        !iv_method       TYPE string
        !iv_filename     TYPE rlgrap-filename
        !iv_datax        TYPE xstring OPTIONAL
      RETURNING
        VALUE(rs_return) TYPE ty_http_return .
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_OBS IMPLEMENTATION.


  METHOD call_obs_api.
    DATA: lv_filename    TYPE string,
          lv_sysubrc     LIKE sy-subrc,
          lv_error_text  TYPE string,
          lr_http_client TYPE REF TO if_http_client.
    DATA: lt_headers       TYPE tihttpnvp.
    DATA: ls_signature TYPE ty_signature.
    "文件名转码,否则中文会有问题
    lv_filename = cl_http_utility=>escape_url( unescaped = CONV #( iv_filename ) ) .
    "根据URL生成HTTP代理
    CALL METHOD cl_http_client=>create_by_url(
      EXPORTING
        url                = |{ gs_conf-zurl }{ lv_filename }|
      IMPORTING
        client             = lr_http_client
      EXCEPTIONS
        argument_not_found = 1
        plugin_not_active  = 2
        internal_error     = 3
        OTHERS             = 4 ).

    IF sy-subrc NE 0.
      rs_return-msg = '生成HTTP代理错误'.
      RETURN.
    ENDIF.
*设置 HTTP 版本
*    LR_HTTP_CLIENT->REQUEST->SET_VERSION( IF_HTTP_REQUEST=>CO_PROTOCOL_VERSION_1_0 ).

    CHECK lr_http_client IS NOT INITIAL.
    ls_signature-filename = lv_filename.
    ls_signature-method = iv_method.
*将HTTP代理设置PUT方法
    lr_http_client->request->set_method( iv_method ).
* 设置HEADER
    lr_http_client->request->set_content_type( 'text/plain' ).
    IF gs_conf-zpublic = abap_false.
      get_signature( CHANGING cs_signature = ls_signature ).
      IF ls_signature-signature IS INITIAL.
        rs_return-msg = '获取签名失败'.
      ENDIF.
      lr_http_client->request->set_header_field( name = 'AUTHORIZATION' value = ls_signature-signature ).
    ENDIF.
    lr_http_client->request->set_header_field( name = 'DATE' value = ls_signature-date ).
    IF iv_method = 'PUT' AND iv_datax IS NOT INITIAL.
      lr_http_client->request->set_data( iv_datax ).
    ENDIF.
    "发送HTTP请求
    CALL METHOD lr_http_client->send
*    EXPORTING
*      TIMEOUT                    = 60  "设置超时值 60S
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2.

    CALL METHOD lr_http_client->receive
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3.

    IF sy-subrc NE 0.
*返回错误连接文本
      CALL METHOD lr_http_client->get_last_error
        IMPORTING
          code    = lv_sysubrc
          message = lv_error_text.
      rs_return-msg = lv_error_text.
    ELSE.
* 获取返回的数据
      IF iv_method = 'GET'.
        rs_return-responsex = lr_http_client->response->get_data( ).
      ELSE.
        rs_return-response = lr_http_client->response->get_cdata( ).
      ENDIF.
      lr_http_client->response->get_header_fields( CHANGING fields = lt_headers ).
      lr_http_client->response->get_status( IMPORTING code = rs_return-status_code reason = rs_return-status_reason ).
      "删除成功
      IF iv_method = 'DELETE' AND rs_return-status_code = '204'.
        rs_return-status_code = '200'.
      ENDIF.
*关闭HTTP链接
      lr_http_client->close( ).
    ENDIF.
  ENDMETHOD.


  METHOD class_constructor.
    IF gs_conf IS INITIAL.
      SELECT SINGLE * INTO gs_conf FROM ztobs_conf WHERE zpublic = 'X'.
    ENDIF.
  ENDMETHOD.


  METHOD constructor.
    SELECT SINGLE * INTO gs_conf FROM ztobs_conf WHERE zbucket = iv_bucket.
  ENDMETHOD.


  METHOD convert_gmt_date.
    "FRI, 01 DEC 2023 01:45:47 GMT
    DATA: lv_date  TYPE d,
          lv_time  TYPE t,
          lv_month TYPE char3,
          lv_day   TYPE char3.
    DATA: lv_wotnr TYPE scal-indicator.
    IF iv_timestamp IS INITIAL.
      GET TIME STAMP FIELD iv_timestamp.
    ENDIF.
    CONVERT TIME STAMP iv_timestamp TIME ZONE 'UTC'
        INTO DATE lv_date TIME lv_time.

    CALL FUNCTION 'DATE_COMPUTE_DAY'
      EXPORTING
        date = lv_date
      IMPORTING
        day  = lv_wotnr.
    SELECT SINGLE left( ltx,3 ) FROM t247 WHERE spras = 'E' AND mnr = @lv_date+4(2) INTO @lv_month .
    SELECT SINGLE left( langt,3 ) FROM t246 WHERE sprsl = 'E' AND wotnr = @lv_wotnr INTO @lv_day .
    rv_date = |{ lv_day }, { lv_date+6(2) } { lv_month } { lv_date(4) } { lv_time TIME = ISO } GMT|.
  ENDMETHOD.


  METHOD download_file_from_obs.
    DATA: lv_filename    TYPE rlgrap-filename.
    DATA(ls_return) = read_file_from_obs( iv_filename = iv_filename ).
    IF ls_return-return-type = 'S'.
      lv_filename = zcl_common=>save_file_dialog( iv_filename = iv_filename ).
      zcl_common=>download_file( iv_filename = lv_filename data_tab = ls_return-data_tab ).
      rs_return = VALUE #( id = '00' type = 'S' number = '001' message = '文件下载成功' ) .
    ELSE.
      rs_return = ls_return-return.
    ENDIF.

  ENDMETHOD.


  METHOD get_signature.
    DATA: lv_data TYPE string,
          lv_keyx TYPE xstring.
    CLEAR cs_signature-signature.
    cs_signature-date = convert_gmt_date(  ).
    lv_data = |{ cs_signature-method }\n\ntext/plain\n{ cs_signature-date }\n/{ gs_conf-zbucket }/{ cs_signature-filename }|.
    lv_keyx = zcl_common=>string_to_xstring( CONV #( gs_conf-zsecret ) ).
    TRY.
        cl_abap_hmac=>calculate_hmac_for_char( EXPORTING if_key = lv_keyx if_data = lv_data IMPORTING ef_hmacb64string = cs_signature-signature ).
        cs_signature-signature = |OBS { gs_conf-zaccess }:{ cs_signature-signature }|.
      CATCH cx_abap_message_digest.
    ENDTRY.
  ENDMETHOD.


  METHOD read_file_from_obs.
    DATA: lt_data_tab TYPE  zcl_common=>tt_pic_tab.

    DATA(ls_return) = call_obs_api( iv_method = 'GET' iv_filename = iv_filename ).
    IF ls_return-status_code = 200.
      lt_data_tab = zcl_common=>xstring_to_binary( ls_return-responsex ).
      rs_return = VALUE #( data_tab = lt_data_tab return = VALUE #( id = '00' type = 'S' number = '001' message = '文件下载成功' ) ).
    ELSEIF ls_return-msg IS NOT INITIAL.
      rs_return = VALUE #( return = VALUE #( id = '00' type = 'E' number = '001' message = ls_return-msg ) ).
    ELSE.
      rs_return = VALUE #( return = VALUE #( id = '00' type = 'E' number = '001' message = ls_return-status_reason ) ).
    ENDIF.
  ENDMETHOD.


  METHOD remove_file_from_obs.
    DATA(ls_return1) = call_obs_api( iv_method = 'DELETE' iv_filename = iv_filename ).
    IF ls_return1-status_code = 200.
      rs_return = VALUE #( id = '00' type = 'S' number = '001' message = '文件删除成功' ) .
    ELSEIF ls_return1-msg IS NOT INITIAL.
      rs_return = VALUE #( id = '00' type = 'E' number = '001' message = ls_return1-msg ) .
    ELSE.
      rs_return = VALUE #( id = '00' type = 'E' number = '001' message = ls_return1-status_reason ) .
    ENDIF.
  ENDMETHOD.


  METHOD show_picture_from_obs.
    CALL FUNCTION 'ZFUN_DISPLAY_PICTURE_OBS'
      EXPORTING
        iv_filename = iv_filename
        io_picture  = io_picture.
  ENDMETHOD.


  METHOD upload_file_to_obs.
    DATA: lv_filename    TYPE rlgrap-filename.
    DATA: lv_datax     TYPE xstring.

    lv_filename = zcl_common=>get_file_name( iv_filter = cl_gui_frontend_services=>filetype_all ).
    DATA(ls_split_file) = zcl_common=>split_file( lv_filename ).
    "文件名加日期前缀
    ls_split_file-filename = |{ sy-datum }{ sy-uzeit }_{ ls_split_file-filename }|.
    DATA(lv_ext) =  to_lower( ls_split_file-pure_extension ).
    IF gs_conf-zpublic = abap_true AND NOT ( lv_ext = 'gif' OR lv_ext = 'jpg' OR lv_ext = 'jpeg' OR lv_ext = 'png' OR lv_ext = 'bmp' ).
      rs_return = VALUE #( return = VALUE #( id = '00' type = 'E' number = '001' message = '文件格式错误' ) ).
      RETURN.
    ENDIF.
    DATA(ls_return) = zcl_common=>upload_file( iv_filename = lv_filename ).
    IF ls_return-subrc <> 0.
      rs_return = VALUE #( return = VALUE #( id = '00' type = 'E' number = '001' message = '文件读取错误' ) ).
      RETURN.
    ENDIF.
    lv_datax = zcl_common=>binary_to_xstring( iv_filelength = ls_return-filelength data_tab = ls_return-data_tab ).
    DATA(ls_return1) = call_obs_api( iv_method = 'PUT' iv_filename = ls_split_file-filename iv_datax = lv_datax ).
    IF ls_return1-status_code = 200.
      rs_return = VALUE #( filename = ls_split_file-filename url = |{ gs_conf-zurl }{ ls_split_file-filename }| return = VALUE #( id = '00' type = 'S' number = '001' message = '文件上传成功' ) ).
    ELSEIF ls_return1-msg IS NOT INITIAL.
      rs_return = VALUE #( return = VALUE #( id = '00' type = 'E' number = '001' message = ls_return1-msg ) ).
    ELSE.
      rs_return = VALUE #( return = VALUE #( id = '00' type = 'E' number = '001' message = ls_return1-status_reason ) ).
    ENDIF.
  ENDMETHOD.
ENDCLASS.
