from pydantic import BaseModel, Field


class RealEstateComplex(BaseModel):
    """
    아파트 단지 정보
    """
    complex_no: str = Field(..., alias='complexNo')
    complex_name: str = Field(..., alias='complexName')
    cortar_no: str = Field(..., alias='cortarNo')
    real_estate_type_code: str = Field(..., alias='realEstateTypeCode')
    real_estate_type_name: str = Field(..., alias='realEstateTypeName')
    latitude: float
    longitude: float
    total_household_count: int = Field(..., alias='totalHouseholdCount')
    # high_floor: int = Field(..., alias='highFloor')
    # low_floor: int = Field(..., alias='lowFloor')
    use_approve_ymd: str = Field(..., alias='useApproveYmd')
    total_dong_count: int = Field(..., alias='totalDongCount')
    max_supply_area: float = Field(..., alias='maxSupplyArea')
    min_supply_area: float = Field(..., alias='minSupplyArea')
    max_total_area: float = Field(..., alias='maxTotalArea')
    min_total_area: float = Field(..., alias='minTotalArea')
    isale_deal_restriction_code: str = Field(..., alias='isaleDealRestrictionCode')
    rebuild_membership_trans_yn: str = Field(..., alias='rebuildMembershipTransYn')
    cortar_address: str = Field(..., alias='cortarAddress')
    deep_link: str = Field(..., alias='deepLink')
    
    
    class Config:
        """
        외부 입력 데이터는 원래의 camelCase 키를 사용하지만, 
        내부에서는 snake_case로 접근할 수 있어 파이썬 스타일에 맞게 사용
        """
        allow_population_by_field_name = True
        extra = "ignore"  # 모델에 정의되지 않은 필드는 무시합니다.


class RealEstateArticle(BaseModel):
    """_summary_

    아파트 매물 정보
    A1: 매매, B1: 전세
    """
    article_no: str = Field(..., alias='articleNo', title="매물 번호") # 중요
    article_name: str = Field(..., alias='articleName')
    article_status: str = Field(..., alias='articleStatus')
    real_estate_type_code: str = Field(..., alias='realEstateTypeCode')
    area1: int = Field(..., alias='area1')
    area2: int = Field(..., alias='area2')
    area_name: str = Field(..., alias='areaName')
    article_confirm_ymd: str = Field(..., alias='articleConfirmYmd', title="매물 확인 일자") # 중요
    article_feature_desc: str = Field(..., alias='articleFeatureDesc')
    article_real_estate_type_code: str = Field(..., alias='articleRealEstateTypeCode')
    article_real_estate_type_name: str = Field(..., alias='articleRealEstateTypeName')
    building_name: str = Field(..., alias='buildingName')
    cp_mobile_article_link_use_at_article_title_yn: bool = Field(..., alias='cpMobileArticleLinkUseAtArticleTitleYn')
    cp_mobile_article_link_use_at_cp_name_yn: bool = Field(..., alias='cpMobileArticleLinkUseAtCpNameYn')
    cp_mobile_article_url: str = Field(..., alias='cpMobileArticleUrl')
    cp_name: str = Field(..., alias='cpName')
    cp_pc_article_bridge_url: str = Field(..., alias='cpPcArticleBridgeUrl')
    cp_pc_article_link_use_at_article_title_yn: bool = Field(..., alias='cpPcArticleLinkUseAtArticleTitleYn')
    cp_pc_article_link_use_at_cp_name_yn: bool = Field(..., alias='cpPcArticleLinkUseAtCpNameYn')
    cp_pc_article_url: str = Field(..., alias='cpPcArticleUrl')
    cpid: str = Field(..., alias='cpid')
    deal_or_warrant_prc: str = Field(..., alias='dealOrWarrantPrc')
    detail_address: str = Field(..., alias='detailAddress')
    detail_address_yn: str = Field(..., alias='detailAddressYn')
    direction: str = Field(..., alias='direction')
    elevator_count: int = Field(..., alias='elevatorCount')
    floor_info: str = Field(..., alias='floorInfo')
    is_complex: bool = Field(..., alias='isComplex')
    is_direct_trade: bool = Field(..., alias='isDirectTrade')
    is_interest: bool = Field(..., alias='isInterest')
    is_location_show: bool = Field(..., alias='isLocationShow')
    is_price_modification: bool = Field(..., alias='isPriceModification')
    is_vr_exposed: bool = Field(..., alias='isVrExposed')
    latitude: float = Field(..., alias='latitude')
    longitude: float = Field(..., alias='longitude')
    price_change_state: str = Field(..., alias='priceChangeState')
    realtor_id: str = Field(..., alias='realtorId')
    realtor_name: str = Field(..., alias='realtorName')
    same_addr_cnt: int = Field(..., alias='sameAddrCnt')
    same_addr_direct_cnt: int = Field(..., alias='sameAddrDirectCnt')
    same_addr_max_prc: str = Field(..., alias='sameAddrMaxPrc')
    same_addr_min_prc: str = Field(..., alias='sameAddrMinPrc')
    site_image_count: int = Field(..., alias='siteImageCount')
    tag_list: list[str] = Field(..., alias='tagList')
    trade_checked_by_owner: bool = Field(..., alias='tradeCheckedByOwner')
    trade_type_code: str = Field(..., alias='tradeTypeCode', title="거래 유형 코드(B1)") # 중요
    trade_type_name: str = Field(..., alias='tradeTypeName', description="거래 유형(전세)") # 중요
    verification_type_code: str = Field(..., alias='verificationTypeCode')

    class Config:
        allow_population_by_field_name = True
        extra = "ignore"  # 모델에 정의되지 않은 필드는 무시합니다.