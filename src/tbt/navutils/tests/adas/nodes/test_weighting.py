from ....adas.nodes import weighting
import pandas as pd
from shapely import wkt
from shapely.geometry import LineString

def test_generate_reproducible_uuids():
    provider = 'Genesis'
    id1 = 123
    weight_direction = True
    encoded_id = weighting.generate_reproducible_uuids(provider, id1, weight_direction)
    assert encoded_id == 'bfa3d755-a4e2-3f16-0e4c-d3e984999a67'
    encoded_id2 = weighting.generate_reproducible_uuids(provider, id1, weight_direction)
    assert encoded_id2 == 'bfa3d755-a4e2-3f16-0e4c-d3e984999a67'
    encoded_id3 = weighting.generate_reproducible_uuids(provider, id1, weight_direction)
    assert encoded_id3 == 'bfa3d755-a4e2-3f16-0e4c-d3e984999a67'



def test_weight_duplicates():
    # Create a dataframe with 4 rows. And 2 columns feature_id and geom
    # geom1 = LINESTRING (4.727970380406525 51.47930454595261, 4.7279164195 51.4792782068)
    # geom2 = LINESTRING (4.9450600147 51.4478735626, 4.9450197816 51.4478614926)
    # geom3 = LINESTRING (4.727970380406525 51.47930454595261, 4.7279164195 51.4792782068)
    # geom4 = geom3 reversed coordinates
    # feature1 = 1
    # feature2 = 2
    # feature3 = 1
    # feature4 = 1
    # results = feature 1 weigth 2 direction True
    #           feature 1 weigth 1 direction False
    #           feature 2 weigth 1 direction True
    # Now the reference is the one with lower lat and lot so now the referece is the reversed

    geom1 = wkt.loads("LINESTRING (4.727970380406525 51.47930454595261, 4.7279164195 51.4792782068)")
    geom2 = wkt.loads("LINESTRING (4.9450600147 51.4478735626, 4.9450197816 51.4478614926)")
    geom3 = wkt.loads("LINESTRING (4.727970380406525 51.47930454595261, 4.7279164195 51.4792782068)")
    geom4 = LineString(list(geom3.coords)[::-1])

    input_df = pd.DataFrame(
        {
        'original_trace_id': ['a', 'b', 'c', 'd'],
        'feature_id': [1, 2, 1, 1],
        'geom': [geom1, geom2, geom3, geom4]
        }
    )
    params = {'provider':'genesis'}
    output_df = weighting.weight_duplicates(input_df, params)
    assert len(output_df) == 3 # id 1 true, id 2 true and id 1 false
    assert output_df['weight'].sum() == 4
    assert output_df[(output_df['feature_id']==1)&(output_df['weight_direction']==False)]['weight'].sum() == 2
    assert output_df[(output_df['feature_id']==1)&(output_df['weight_direction']==True)]['weight'].sum() == 1