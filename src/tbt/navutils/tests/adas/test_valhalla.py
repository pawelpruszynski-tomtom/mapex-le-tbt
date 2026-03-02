from ...adas.map_matchers import ValhallaMapMatcher
import requests
import os
import pandas as pd
import pytest
from shapely import wkt

file_path = os.path.dirname(os.path.abspath(__file__))

test_input_data_path = os.path.join(file_path, "test_input_data")


@pytest.fixture
def valhalla_results():
    print("Encontrado")
    print(file_path)
    df = pd.read_csv(os.path.join(test_input_data_path, "valhalla_output.csv"))
    return df


def test_valhalla_polyline():
    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url)
    poly = "ezdvBcps}Lt@W@F@BVKl@UK]Wc@}@kAOKBEzA{BdA}Ab@s@LBf@RjARZBv@?rAAfBIX?f@FLBf@^b@hATbAF~@BvB?hALjBHpCLjAVdAd@dAxCbGlA~BbBvCpAbD`AnCVpB\\hBf@nB|@tAb@f@RVXLbDj@jCl@nAf@`DbBdC`B`@b@HNN`@lA~DnAvBvB`G\\fA^p@j@z@~E`FbBpBxDfFXh@|@pD`@bB^fANd@\\`Bl@rBb@fATj@v@bBjBwCd@w@NB^B\\@\\K\\Q|AWZGXOPIRBb@K\\Kl@_@VOZWVI^Eh@Ov@KxCJ\\@\\K^@RMh@QhAMXKXY`@Q^SLK`@]fBc@n@M\\OlB_@n@KNMRUZQZGh@Ht@Lx@F`@BTANDRNTPNDDHLXN`@LRTJx@?RBJFFLV\\Zs@TQ`@MVIhAYp@YPc@VgALk@Hk@d@eA\\cAHq@Eo@J}@@q@DItAgAv@i@|@e@v@k@bAaAdDuB|@c@?I?]?{@Ms@COFWJODYEcABOTU^e@H_@Km@Ci@BU?e@Ca@Ie@e@oBKk@Eu@M{@CoAC}@Km@Iy@IaA?{@Ci@Ms@UgA]y@Mm@GQICe@QKMGi@Gi@GSI[Ym@[{@@_AK]SMo@i@OCg@EOEMMYSi@IUO]Ko@KSIWUWMU_@I]Kw@WoACY@a@CWQWe@[QIOYMYW]SMQUQu@OWWSUWU[SsAEg@Ic@Mc@?a@Ai@Em@AUOa@M_@e@g@MSGYOY_@YIQSR[Pa@mBQaAI[@QDyACq@Iu@o@k@e@Y]IcAAmBC]Ee@IgAUWAw@Ae@GWSaAeAYk@Si@a@k@YAc@O{@]a@MnAqA~@mCVaAHk@YmCIQSQOS"
    result = map_match_engine.decode_valhalla_polyline(poly)
    assert result[0] == [7.317778, 1.952691]
    assert result[1] == [7.31779, 1.952664]



def test_trace_attributes_req(mocker, valhalla_results):

    r = requests.Response()
    r.status_code=200
    valhalla_content = b'{"units":"kilometers","shape":"_vkuaB{fb`d@??zDegA^iKd@gN`@yNBkAD_BF_AXgINcG?oDI_DU{Bi@yBy@yB{BsCkM|DiLlDoBl@yRdGiK`DyCdF}@lBm@dB]vAI~@GxA@zALrCj@bKr@hJDd@Bd@JbAFz@|@tLlA`MTfC`@rEbAhLj@|E~@bIHz@N`AzC`SxA|HfA|F|C`N~FbV`BrJlAfLh@`MCxO}@fXgBnj@ItBGxBg@zPe@fOoBlm@s@jT","edges":[{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":72947742841,"traversability":"forward","end_shape_index":4,"begin_shape_index":0,"road_class":"tertiary","speed":50,"length":0.11,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":73014851705,"traversability":"forward","end_shape_index":6,"begin_shape_index":4,"road_class":"tertiary","speed":50,"length":0.02,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":335813163129,"traversability":"forward","end_shape_index":7,"begin_shape_index":6,"road_class":"tertiary","speed":50,"length":0.003,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":218204878969,"traversability":"forward","end_shape_index":9,"begin_shape_index":7,"road_class":"tertiary","speed":50,"length":0.014,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":102157795,"id":37413599353,"traversability":"forward","end_shape_index":16,"begin_shape_index":9,"road_class":"tertiary","speed":50,"length":0.044,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":3,"way_id":1085860487,"id":37749143673,"traversability":"forward","end_shape_index":17,"begin_shape_index":16,"road_class":"secondary","speed":50,"length":0.026,"names":["Aleja Genera\xc5\x82a Zygmunta Waltera-Janke"]},{"speed_limit":50,"lane_count":3,"way_id":203706918,"id":151465113721,"traversability":"forward","end_shape_index":19,"begin_shape_index":17,"road_class":"secondary","speed":50,"length":0.031,"names":["Aleja Genera\xc5\x82a Zygmunta Waltera-Janke"]},{"speed_limit":50,"lane_count":3,"way_id":1085860484,"id":171430000761,"traversability":"forward","end_shape_index":20,"begin_shape_index":19,"road_class":"secondary","speed":50,"length":0.036,"names":["Aleja ksi\xc4\x99dza biskupa W\xc5\x82adys\xc5\x82awa Bandurskiego"]},{"speed_limit":50,"lane_count":3,"way_id":1085860484,"id":31138920569,"traversability":"forward","end_shape_index":21,"begin_shape_index":20,"road_class":"secondary","speed":50,"length":0.023,"names":["Aleja ksi\xc4\x99dza biskupa W\xc5\x82adys\xc5\x82awa Bandurskiego"]},{"speed_limit":50,"lane_count":1,"way_id":312839720,"id":97241151609,"traversability":"forward","end_shape_index":30,"begin_shape_index":21,"road_class":"tertiary","speed":50,"length":0.052,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":31776454777,"traversability":"forward","end_shape_index":32,"begin_shape_index":30,"road_class":"tertiary","speed":50,"length":0.014,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":313130367097,"traversability":"forward","end_shape_index":34,"begin_shape_index":32,"road_class":"tertiary","speed":50,"length":0.004,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":313197475961,"traversability":"forward","end_shape_index":37,"begin_shape_index":34,"road_class":"tertiary","speed":50,"length":0.034,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":101770999929,"traversability":"forward","end_shape_index":43,"begin_shape_index":37,"road_class":"tertiary","speed":50,"length":0.049,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":36373411961,"traversability":"forward","end_shape_index":45,"begin_shape_index":43,"road_class":"tertiary","speed":50,"length":0.026,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":207299688569,"traversability":"forward","end_shape_index":56,"begin_shape_index":45,"road_class":"tertiary","speed":50,"length":0.214,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":332893927545,"traversability":"forward","end_shape_index":58,"begin_shape_index":56,"road_class":"tertiary","speed":50,"length":0.024,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":101603227769,"traversability":"forward","end_shape_index":60,"begin_shape_index":58,"road_class":"tertiary","speed":50,"length":0.07,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":36440520825,"traversability":"forward","end_shape_index":61,"begin_shape_index":60,"road_class":"tertiary","speed":50,"length":0.023,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]}],"alternate_paths":[]}'
    type(r).content = mocker.PropertyMock(return_value=valhalla_content)
    
    mocker.patch("requests.post", return_value=r)

    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url)

    results = map_match_engine.map_match_request(wkt.loads('LINESTRING (19.416702 51.747184, 19.417857 51.74709, 19.41186 51.747549)'))

    assert valhalla_results['length'].sum() == pytest.approx(results['length'].sum(), abs=5)
    assert len(results) == len(valhalla_results)



def test_filter_ferry(mocker):

    r = requests.Response()
    r.status_code=200
    valhalla_content = b'{"units":"kilometers","shape":"sgweiBancig@mlGj}Jy_M~qKmwO|eNqnMp~IgbKbcJ}aC~kF}aEzfOm_FziX_BxfA`IpoDqIn}c@m|B~s|Bthro@nvsgJdk}Ahd{{@x`c@vyz^hckCtzb]{i@ptuUnuqIzbrZng~DnjtBv~pB|li@rah@n}hC~mDf|xDbyh@bfhL_y{Bz}|La}{Bd}~Lw{v@n}hE{ah@bbmCpaMdc~FtxU|b_LrdVla|Kt`K~vhCfcp@xg~Cfqd@hjaChwa@zsvArdd@|f}AlsdA`joArlw@t}t@vxTnlQzbZ|wExa\\\\be`@zaOrpMfsRbwLzkTflD`fOfoAv`AaFrn@yd@rWee@pHicAeEkeAwv@u}Bvv@t}BdEjeAqHhcAsWde@sn@xd@w`A`FafOgoA{kTglDgsRcwL{aOspMya\\\\ce`@{bZ}wEwxTolQslw@u}t@msdAajoAsdd@}f}Aiwa@{svAgqd@ijaCgcp@yg~Cu`K_whCsdVma|KuxU}b_LqaMec~Fzah@cbmCv{v@o}hE`}{Be}~L~x{B{}|Lcyh@cfhL_nDg|xDsah@o}hCw~pB}li@og~DojtBouqI{brZzi@qtuUickCuzb]y`c@wyz^ek}Aid{{@uhro@ovsgJl|B_t|BpIo}c@aIqoD~AyfAl_F{iX|aE{fO|aC_lFfbKccJpnMq~IlwO}eNx_M_rKt~HudMu~HtdMy_M~qKmwO|eNqnMp~IgbKbcJ}aC~kF}aEzfOm_FziX_BxfA`IpoDqIn}c@m|B~s|Bthro@nvsgJdk}Ahd{{@x`c@vyz^hckCtzb]{i@ptuUnuqIzbrZng~DnjtBv~pB|li@rah@n}hC~mDf|xDbyh@bfhL_y{Bz}|La}{Bd}~Lw{v@n}hE{ah@bbmCpaMdc~FtxU|b_LrdVla|Kt`K~vhCfcp@xg~Cfqd@hjaChwa@zsvArdd@|f}AlsdA`joArlw@t}t@vxTnlQzbZ|wExa\\\\be`@zaOrpMfsRbwLzkTflD`fOfoAv`AaFrn@yd@rWee@pHicAeEkeAwv@u}Bvv@t}BdEjeAqHhcAsWde@sn@xd@w`A`FafOgoA{kTglDgsRcwL{aOspMya\\\\ce`@{bZ}wEwxTolQslw@u}t@msdAajoAsdd@}f}Aiwa@{svAgqd@ijaCgcp@yg~Cu`K_whCsdVma|KuxU}b_LqaMec~Fzah@cbmCv{v@o}hE`}{Be}~L~x{B{}|Lcyh@cfhL_nDg|xDsah@o}hCw~pB}li@og~DojtBouqI{brZzi@qtuUickCuzb]y`c@wyz^ek}Aid{{@uhro@ovsgJl|B_t|BpIo}c@aIqoD~AyfAl_F{iX|aE{fO|aC_lFfbKccJpnMq~IlwO}eNx_M_rKt~HudMj^ii@pD_DvRyNpAaB|@{Bv@_Dn@sDJmBNkCRsLa@}GeK{}Ag@qD_AkCm@kAw@s@cBi@uAA}{D|fA{C`BcBhDeA~Fk@fG_BvSeAxEqAtC}BdAcDtAsB?qAc@mAmAcAaE{@qFmW}|AsBsMaBeM_@cGMgGBqGXyEj@}Fz@}ErAcFvAwDhBeDlPwR~GoHpEeEnBuAzAZfBtAnGrS`FhTtAbFtA`ExAfArACpAu@bn@yp@nb@wb@zGeHtOiVpGyGjL_OxDsBpNcAlMqAdUyCfEg@pFq@dAaA|@_Bf@sCNqDQwNsDwxA","edges":[{"lane_count":1,"way_id":3743240318,"id":1625309603536,"use":"ferry","traversability":"both","end_shape_index":50,"begin_shape_index":0,"road_class":"primary","speed":30,"length":751.297,"names":["Klaip\xc4\x97da \xe2\x80\x93 Kiel"]},{"lane_count":1,"way_id":3743240318,"id":26296,"use":"ferry","traversability":"both","end_shape_index":100,"begin_shape_index":50,"road_class":"primary","speed":30,"length":751.412,"names":["Klaip\xc4\x97da \xe2\x80\x93 Kiel"]},{"lane_count":1,"way_id":3743240318,"id":1625309603536,"use":"ferry","traversability":"both","end_shape_index":150,"begin_shape_index":100,"road_class":"primary","speed":30,"length":751.412,"names":["Klaip\xc4\x97da \xe2\x80\x93 Kiel"]},{"lane_count":1,"way_id":3743240318,"id":26296,"use":"ferry","traversability":"both","end_shape_index":200,"begin_shape_index":150,"road_class":"primary","speed":30,"length":751.412,"names":["Klaip\xc4\x97da \xe2\x80\x93 Kiel"]},{"lane_count":1,"way_id":1104643074,"id":1625343157968,"use":"driveway","traversability":"both","end_shape_index":208,"begin_shape_index":200,"road_class":"primary","speed":10,"length":0.146},{"lane_count":1,"way_id":647102572,"id":45070313250,"use":"driveway","traversability":"forward","end_shape_index":210,"begin_shape_index":208,"road_class":"service_other","speed":10,"length":0.018,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":697182070,"id":496343869218,"use":"driveway","traversability":"both","end_shape_index":211,"begin_shape_index":210,"road_class":"service_other","speed":50,"length":0.009,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":3394383466,"id":916579575586,"use":"driveway","traversability":"forward","end_shape_index":226,"begin_shape_index":211,"road_class":"service_other","speed":50,"length":0.541,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":1247591499,"id":890943989538,"use":"driveway","traversability":"forward","end_shape_index":262,"begin_shape_index":226,"road_class":"service_other","speed":50,"length":0.651,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":294912446,"id":41211553570,"use":"driveway","traversability":"forward","end_shape_index":263,"begin_shape_index":262,"road_class":"service_other","speed":50,"length":0.018,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":89303539,"id":1112503904034,"use":"driveway","traversability":"forward","end_shape_index":264,"begin_shape_index":263,"road_class":"service_other","speed":50,"length":0.029,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":421500947,"id":538421126946,"use":"driveway","traversability":"forward","end_shape_index":265,"begin_shape_index":264,"road_class":"service_other","speed":50,"length":0.011,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":133074483,"id":947080554274,"use":"driveway","traversability":"forward","end_shape_index":266,"begin_shape_index":265,"road_class":"service_other","speed":50,"length":0.028,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":1073250432,"id":1110524192546,"use":"driveway","traversability":"forward","end_shape_index":267,"begin_shape_index":266,"road_class":"service_other","speed":50,"length":0.026,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":245888094,"id":68491306786,"use":"driveway","traversability":"forward","end_shape_index":268,"begin_shape_index":267,"road_class":"service_other","speed":50,"length":0.04,"names":["Baltijos prospektas"]},{"speed_limit":50,"lane_count":1,"way_id":1336205028,"id":1709464118992,"use":"road","traversability":"forward","end_shape_index":276,"begin_shape_index":268,"road_class":"trunk","speed":50,"length":0.151,"names":["Baltijos prospektas"]}],"alternate_paths":[]}'

    type(r).content = mocker.PropertyMock(return_value=valhalla_content)
    
    mocker.patch("requests.post", return_value=r)

    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url)

    results = map_match_engine.map_match_request(wkt.loads('LINESTRING (21.13772 55.68529, 21.13765 55.68529, 21.13763 55.68528, 21.13762 55.68528, 21.13761 55.68528, 21.13761 55.68527, 21.1376 55.68527, 21.13759 55.68526, 21.13758 55.68526, 21.13758 55.68525, 21.13757 55.68525, 21.13758 55.68525, 21.13757 55.68525, 21.13758 55.68525, 21.13759 55.68525, 21.13758 55.68525, 21.13757 55.68525, 21.13754 55.68524, 21.13757 55.68525, 21.13759 55.68525, 21.1376 55.68524, 21.13759 55.68524, 21.13759 55.68523, 21.13758 55.68522, 21.13758 55.68523, 21.13758 55.68522, 21.13757 55.68522, 21.13757 55.68521, 21.13758 55.68521, 21.13759 55.68521, 21.13759 55.68522, 21.13758 55.68522, 21.13759 55.68522, 21.13759 55.68521, 21.1376 55.68522, 21.13761 55.68517, 21.13763 55.68516, 21.13763 55.68513, 21.13766 55.68512, 21.1377 55.68508, 21.1377 55.6851, 21.13775 55.68511, 21.13777 55.68512, 21.1378 55.68512, 21.13781 55.68512, 21.13778 55.68511, 21.13774 55.6851, 21.13773 55.68509, 21.1377 55.68511, 21.13763 55.68514, 21.1376 55.68514, 21.13758 55.68515, 21.13757 55.68515, 21.13751 55.68515, 21.13743 55.68515, 21.13739 55.68515, 21.1374 55.68518, 21.13749 55.68516, 21.13754 55.68515, 21.13757 55.68515, 21.13759 55.68516, 21.13758 55.68522, 21.13755 55.68526, 21.13753 55.68527, 21.1376 55.68526, 21.13764 55.68529, 21.13767 55.68529, 21.13773 55.68526, 21.13769 55.68526, 21.13765 55.68525, 21.13763 55.68521, 21.13756 55.68515, 21.13756 55.68507, 21.13759 55.685, 21.13761 55.68495, 21.13763 55.68492, 21.13767 55.68489, 21.13776 55.68486, 21.13785 55.68483, 21.13796 55.68481, 21.13809 55.68474, 21.13819 55.6847, 21.13826 55.68464, 21.13835 55.6846, 21.13851 55.68458, 21.13864 55.68456, 21.1387 55.68451, 21.13879 55.68447, 21.13887 55.68442, 21.13891 55.68436, 21.13894 55.68431, 21.13896 55.68429, 21.13899 55.68427, 21.13901 55.68425, 21.13904 55.68422, 21.13905 55.68419, 21.13904 55.6842, 21.13911 55.68426, 21.13922 55.68431, 21.1391 55.68437, 21.13905 55.68434, 21.13913 55.68425, 21.13928 55.68414, 21.13946 55.68399, 21.13963 55.68381, 21.13977 55.6836, 21.13996 55.68343, 21.14027 55.68337, 21.1407 55.6834, 21.14108 55.68345, 21.14151 55.68349, 21.14192 55.68355, 21.14231 55.68361, 21.14241 55.68382, 21.14228 55.68411, 21.14214 55.68445, 21.142 55.68479, 21.14186 55.68513, 21.14173 55.6855, 21.14159 55.68585, 21.14145 55.68619, 21.14129 55.68656, 21.14113 55.68691, 21.14069 55.68704, 21.14029 55.68714, 21.14018 55.68735, 21.14041 55.68752, 21.14094 55.68763, 21.14163 55.6878, 21.14237 55.68799, 21.14304 55.688, 21.14354 55.6878, 21.14379 55.68756, 21.14395 55.68741, 21.1441 55.68723, 21.14403 55.68704, 21.14362 55.68695, 21.1433 55.68686, 21.14309 55.68677, 21.14306 55.68671, 21.14306 55.68668, 21.1432 55.68649, 21.1435 55.68623, 21.14382 55.68594, 21.14411 55.68565, 21.14435 55.68542, 21.14452 55.68525, 21.14461 55.68515, 21.14471 55.68505, 21.14479 55.68497, 21.14484 55.68495, 21.14498 55.68483, 21.14517 55.68464, 21.14542 55.68444, 21.14551 55.68414, 21.14557 55.68379, 21.14564 55.6835, 21.14584 55.68331, 21.14635 55.68332, 21.14699 55.68335, 21.14761 55.68339)'))
    different_uses = list(results['use'].unique())
    assert "ferry" not in different_uses



def test_trace_attributes_req_error_call_valhalla(mocker):

    r = requests.Response()
    r.status_code=400
    valhalla_content = b'{"edges":[],"shape":"","alternate_paths":[],"units":"kilometers"}'
    type(r).content = mocker.PropertyMock(return_value=valhalla_content)
    mocker.patch("requests.post", return_value=r)

    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url)

    results = map_match_engine.map_match_request(wkt.loads('LINESTRING (19.416702 51.747184, 19.417857 51.74709, 19.41186 51.747549)'))

    assert len(results) == 0


def test_trace_attributes_df(mocker, valhalla_results):

    r = requests.Response()
    r.status_code=200
    valhalla_content = b'{"units":"kilometers","shape":"_vkuaB{fb`d@??zDegA^iKd@gN`@yNBkAD_BF_AXgINcG?oDI_DU{Bi@yBy@yB{BsCkM|DiLlDoBl@yRdGiK`DyCdF}@lBm@dB]vAI~@GxA@zALrCj@bKr@hJDd@Bd@JbAFz@|@tLlA`MTfC`@rEbAhLj@|E~@bIHz@N`AzC`SxA|HfA|F|C`N~FbV`BrJlAfLh@`MCxO}@fXgBnj@ItBGxBg@zPe@fOoBlm@s@jT","edges":[{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":72947742841,"traversability":"forward","end_shape_index":4,"begin_shape_index":0,"road_class":"tertiary","speed":50,"length":0.11,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":73014851705,"traversability":"forward","end_shape_index":6,"begin_shape_index":4,"road_class":"tertiary","speed":50,"length":0.02,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":335813163129,"traversability":"forward","end_shape_index":7,"begin_shape_index":6,"road_class":"tertiary","speed":50,"length":0.003,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":218204878969,"traversability":"forward","end_shape_index":9,"begin_shape_index":7,"road_class":"tertiary","speed":50,"length":0.014,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":102157795,"id":37413599353,"traversability":"forward","end_shape_index":16,"begin_shape_index":9,"road_class":"tertiary","speed":50,"length":0.044,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":3,"way_id":1085860487,"id":37749143673,"traversability":"forward","end_shape_index":17,"begin_shape_index":16,"road_class":"secondary","speed":50,"length":0.026,"names":["Aleja Genera\xc5\x82a Zygmunta Waltera-Janke"]},{"speed_limit":50,"lane_count":3,"way_id":203706918,"id":151465113721,"traversability":"forward","end_shape_index":19,"begin_shape_index":17,"road_class":"secondary","speed":50,"length":0.031,"names":["Aleja Genera\xc5\x82a Zygmunta Waltera-Janke"]},{"speed_limit":50,"lane_count":3,"way_id":1085860484,"id":171430000761,"traversability":"forward","end_shape_index":20,"begin_shape_index":19,"road_class":"secondary","speed":50,"length":0.036,"names":["Aleja ksi\xc4\x99dza biskupa W\xc5\x82adys\xc5\x82awa Bandurskiego"]},{"speed_limit":50,"lane_count":3,"way_id":1085860484,"id":31138920569,"traversability":"forward","end_shape_index":21,"begin_shape_index":20,"road_class":"secondary","speed":50,"length":0.023,"names":["Aleja ksi\xc4\x99dza biskupa W\xc5\x82adys\xc5\x82awa Bandurskiego"]},{"speed_limit":50,"lane_count":1,"way_id":312839720,"id":97241151609,"traversability":"forward","end_shape_index":30,"begin_shape_index":21,"road_class":"tertiary","speed":50,"length":0.052,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":31776454777,"traversability":"forward","end_shape_index":32,"begin_shape_index":30,"road_class":"tertiary","speed":50,"length":0.014,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":313130367097,"traversability":"forward","end_shape_index":34,"begin_shape_index":32,"road_class":"tertiary","speed":50,"length":0.004,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":313197475961,"traversability":"forward","end_shape_index":37,"begin_shape_index":34,"road_class":"tertiary","speed":50,"length":0.034,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":101770999929,"traversability":"forward","end_shape_index":43,"begin_shape_index":37,"road_class":"tertiary","speed":50,"length":0.049,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":36373411961,"traversability":"forward","end_shape_index":45,"begin_shape_index":43,"road_class":"tertiary","speed":50,"length":0.026,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":207299688569,"traversability":"forward","end_shape_index":56,"begin_shape_index":45,"road_class":"tertiary","speed":50,"length":0.214,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":332893927545,"traversability":"forward","end_shape_index":58,"begin_shape_index":56,"road_class":"tertiary","speed":50,"length":0.024,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":101603227769,"traversability":"forward","end_shape_index":60,"begin_shape_index":58,"road_class":"tertiary","speed":50,"length":0.07,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":36440520825,"traversability":"forward","end_shape_index":61,"begin_shape_index":60,"road_class":"tertiary","speed":50,"length":0.023,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]}],"alternate_paths":[]}'
    type(r).content = mocker.PropertyMock(return_value=valhalla_content)
    mocker.patch("requests.post", return_value=r)

    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url, test_sleep_time=0.1)

    df = pd.DataFrame(data={'geom':['LINESTRING (19.416702 51.747184, 19.417857 51.74709, 19.41186 51.747549)'], 'id':'id1'})

    results = map_match_engine.map_match_df(df)

    assert len(results['id'].unique())==1
    assert results['id'].iloc[0]=='id1'

    assert pytest.approx(results['length'].sum(),abs=5) ==  valhalla_results['length'].sum()
    assert len(results) == len(valhalla_results)


def test_trace_attributes_df_repeated_cols(mocker, valhalla_results):

    r = requests.Response()
    r.status_code=200
    valhalla_content = b'{"units":"kilometers","shape":"_vkuaB{fb`d@??zDegA^iKd@gN`@yNBkAD_BF_AXgINcG?oDI_DU{Bi@yBy@yB{BsCkM|DiLlDoBl@yRdGiK`DyCdF}@lBm@dB]vAI~@GxA@zALrCj@bKr@hJDd@Bd@JbAFz@|@tLlA`MTfC`@rEbAhLj@|E~@bIHz@N`AzC`SxA|HfA|F|C`N~FbV`BrJlAfLh@`MCxO}@fXgBnj@ItBGxBg@zPe@fOoBlm@s@jT","edges":[{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":72947742841,"traversability":"forward","end_shape_index":4,"begin_shape_index":0,"road_class":"tertiary","speed":50,"length":0.11,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":73014851705,"traversability":"forward","end_shape_index":6,"begin_shape_index":4,"road_class":"tertiary","speed":50,"length":0.02,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":335813163129,"traversability":"forward","end_shape_index":7,"begin_shape_index":6,"road_class":"tertiary","speed":50,"length":0.003,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":165298119,"id":218204878969,"traversability":"forward","end_shape_index":9,"begin_shape_index":7,"road_class":"tertiary","speed":50,"length":0.014,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":102157795,"id":37413599353,"traversability":"forward","end_shape_index":16,"begin_shape_index":9,"road_class":"tertiary","speed":50,"length":0.044,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":3,"way_id":1085860487,"id":37749143673,"traversability":"forward","end_shape_index":17,"begin_shape_index":16,"road_class":"secondary","speed":50,"length":0.026,"names":["Aleja Genera\xc5\x82a Zygmunta Waltera-Janke"]},{"speed_limit":50,"lane_count":3,"way_id":203706918,"id":151465113721,"traversability":"forward","end_shape_index":19,"begin_shape_index":17,"road_class":"secondary","speed":50,"length":0.031,"names":["Aleja Genera\xc5\x82a Zygmunta Waltera-Janke"]},{"speed_limit":50,"lane_count":3,"way_id":1085860484,"id":171430000761,"traversability":"forward","end_shape_index":20,"begin_shape_index":19,"road_class":"secondary","speed":50,"length":0.036,"names":["Aleja ksi\xc4\x99dza biskupa W\xc5\x82adys\xc5\x82awa Bandurskiego"]},{"speed_limit":50,"lane_count":3,"way_id":1085860484,"id":31138920569,"traversability":"forward","end_shape_index":21,"begin_shape_index":20,"road_class":"secondary","speed":50,"length":0.023,"names":["Aleja ksi\xc4\x99dza biskupa W\xc5\x82adys\xc5\x82awa Bandurskiego"]},{"speed_limit":50,"lane_count":1,"way_id":312839720,"id":97241151609,"traversability":"forward","end_shape_index":30,"begin_shape_index":21,"road_class":"tertiary","speed":50,"length":0.052,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":31776454777,"traversability":"forward","end_shape_index":32,"begin_shape_index":30,"road_class":"tertiary","speed":50,"length":0.014,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":313130367097,"traversability":"forward","end_shape_index":34,"begin_shape_index":32,"road_class":"tertiary","speed":50,"length":0.004,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":1,"way_id":28120393,"id":313197475961,"traversability":"forward","end_shape_index":37,"begin_shape_index":34,"road_class":"tertiary","speed":50,"length":0.034,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":101770999929,"traversability":"forward","end_shape_index":43,"begin_shape_index":37,"road_class":"tertiary","speed":50,"length":0.049,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":36373411961,"traversability":"forward","end_shape_index":45,"begin_shape_index":43,"road_class":"tertiary","speed":50,"length":0.026,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":207299688569,"traversability":"forward","end_shape_index":56,"begin_shape_index":45,"road_class":"tertiary","speed":50,"length":0.214,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":332893927545,"traversability":"forward","end_shape_index":58,"begin_shape_index":56,"road_class":"tertiary","speed":50,"length":0.024,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":101603227769,"traversability":"forward","end_shape_index":60,"begin_shape_index":58,"road_class":"tertiary","speed":50,"length":0.07,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]},{"speed_limit":50,"lane_count":2,"way_id":539885052,"id":36440520825,"traversability":"forward","end_shape_index":61,"begin_shape_index":60,"road_class":"tertiary","speed":50,"length":0.023,"names":["Aleja Stefana Wyszy\xc5\x84skiego"]}],"alternate_paths":[]}'
    type(r).content = mocker.PropertyMock(return_value=valhalla_content)
    mocker.patch("requests.post", return_value=r)

    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url, test_sleep_time=0.1)

    df = pd.DataFrame(data={'geom':['LINESTRING (19.416702 51.747184, 19.417857 51.74709, 19.41186 51.747549)'], 'id':'id1', 'way_id':123})

    results = map_match_engine.map_match_df(df)

    assert len(results['id'].unique())==1
    assert results['id'].iloc[0]=='id1'

    assert 'way_id' in results.columns
    assert 'original_way_id' in results.columns

    assert results['length'].sum() == pytest.approx(valhalla_results['length'].sum(), abs=5)
    assert len(results) == len(valhalla_results)



def test_trace_attributes_df_no_content(mocker):

    r = requests.Response()
    r.status_code=200
    valhalla_content = b'{"edges":[],"shape":"","alternate_paths":[],"units":"kilometers"}'
    type(r).content = mocker.PropertyMock(return_value=valhalla_content)
    mocker.patch("requests.post", return_value=r)

    url = "http://localhost:8002"
    map_match_engine = ValhallaMapMatcher(url=url, test_sleep_time=0.1)

    df = pd.DataFrame(data={'geom':['LINESTRING (19.416702 51.747184, 19.417857 51.74709, 19.41186 51.747549)'], 'id':'id1', 'way_id':123})

    results = map_match_engine.map_match_df(df)
    assert len(results) == 0
