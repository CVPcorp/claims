from __future__ import (absolute_import, division, print_function)

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap
from matplotlib.colors import rgb2hex, Normalize
from matplotlib.patches import Polygon
from matplotlib.colorbar import ColorbarBase
import duckdb

def get_readmission_rates():
    with duckdb.connect('claims.duckdb', read_only=True) as conn:
        query = """
        SELECT 
            s.state_name,
            ROUND(CAST(SUM(rr.readmissions) AS FLOAT) / NULLIF(SUM(rr.total_admissions), 0) * 100, 2) AS readmission_rate
        FROM 
            readmission_rate rr
        JOIN 
            state s ON rr.SP_STATE_CODE = s.sp_state_code
        GROUP BY 
            s.state_name
        ORDER BY 
            readmission_rate DESC
        """
        result = conn.execute(query).fetchall()
    return result

def draw_map(readmission_rates):
    fig, ax = plt.subplots(figsize=(5, 3))

    # Lambert Conformal map of lower 48 states.
    m = Basemap(llcrnrlon=-119,llcrnrlat=20,urcrnrlon=-64,urcrnrlat=49,
                projection='lcc',lat_1=33,lat_2=45,lon_0=-95)

    # Mercator projection, for Alaska and Hawaii
    m_ = Basemap(llcrnrlon=-190,llcrnrlat=20,urcrnrlon=-143,urcrnrlat=46,
                projection='merc',lat_ts=20)  # do not change these numbers

    #%% ---------   draw state boundaries  ----------------------------------------
    ## data from U.S Census Bureau
    ## http://www.census.gov/geo/www/cob/st2000.html
    shp_info = m.readshapefile('shapefiles/st99_d00','states',drawbounds=True,
                               linewidth=0.45,color='gray')
    shp_info_ = m_.readshapefile('shapefiles/st99_d00','states',drawbounds=False)

    #%% -------- choose a color for each state based on readmission rate. -------
    colors={}
    statenames=[]
    cmap = plt.cm.YlOrRd  # use YlOrRd colormap
    # Filter out None values before calculating min and max
    valid_rates = [rate for _, rate in readmission_rates if rate is not None]
    vmin = min(valid_rates) if valid_rates else 0
    vmax = max(valid_rates) if valid_rates else 1
    norm = Normalize(vmin=vmin, vmax=vmax)
    for shapedict in m.states_info:
        statename = shapedict['NAME']
        # skip DC and Puerto Rico.
        if statename not in ['District of Columbia','Puerto Rico']:
            rate = next((rate for state, rate in readmission_rates if state == statename), 0)
            # Handle None values by using 0 instead
            if rate is None:
                rate = 0
            colors[statename] = cmap(norm(rate))[:3]
        statenames.append(statename)

    #%% ---------  cycle through state names, color each one.  --------------------
    for nshape,seg in enumerate(m.states):
        # skip DC and Puerto Rico.
        if statenames[nshape] not in ['Puerto Rico', 'District of Columbia']:
            color = rgb2hex(colors[statenames[nshape]])
            poly = Polygon(seg,facecolor=color,edgecolor=color)
            ax.add_patch(poly)

    AREA_1 = 0.005  # exclude small Hawaiian islands that are smaller than AREA_1
    AREA_2 = AREA_1 * 30.0  # exclude Alaskan islands that are smaller than AREA_2
    AK_SCALE = 0.19  # scale down Alaska to show as a map inset
    HI_OFFSET_X = -1900000  # X coordinate offset amount to move Hawaii "beneath" Texas
    HI_OFFSET_Y = 250000    # similar to above: Y offset for Hawaii
    AK_OFFSET_X = -250000   # X offset for Alaska (These four values are obtained
    AK_OFFSET_Y = -750000   # via manual trial and error, thus changing them is not recommended.)

    for nshape, shapedict in enumerate(m_.states_info):  # plot Alaska and Hawaii as map insets
        if shapedict['NAME'] in ['Alaska', 'Hawaii']:
            seg = m_.states[int(shapedict['SHAPENUM'] - 1)]
            if shapedict['NAME'] == 'Hawaii' and float(shapedict['AREA']) > AREA_1:
                seg = [(x + HI_OFFSET_X, y + HI_OFFSET_Y) for x, y in seg]
                color = rgb2hex(colors[statenames[nshape]])
            elif shapedict['NAME'] == 'Alaska' and float(shapedict['AREA']) > AREA_2:
                seg = [(x*AK_SCALE + AK_OFFSET_X, y*AK_SCALE + AK_OFFSET_Y)\
                       for x, y in seg]
                color = rgb2hex(colors[statenames[nshape]])
            poly = Polygon(seg, facecolor=color, edgecolor='gray', linewidth=.45)
            ax.add_patch(poly)

    ax.set_title('United States Readmission Rates by State')

    #%% ---------  Plot bounding boxes for Alaska and Hawaii insets  --------------
    light_gray = [0.8]*3  # define light gray color RGB
    x1,y1 = m_([-190,-183,-180,-180,-175,-171,-171],[29,29,26,26,26,22,20])
    x2,y2 = m_([-180,-180,-177],[26,23,20])  # these numbers are fine-tuned manually
    m_.plot(x1,y1,color=light_gray,linewidth=0.8)  # do not change them drastically
    m_.plot(x2,y2,color=light_gray,linewidth=0.8)

    #%% ---------   Show color bar  ---------------------------------------
    ax_c = fig.add_axes([0.9, 0.1, 0.03, 0.8])
    cb = ColorbarBase(ax_c,cmap=cmap,norm=norm,orientation='vertical',
                      label=r'Readmission Rate')

    return fig

def draw_column_chart(readmission_rates):
    # Remove plt.xkcd() to avoid font warnings
    fig, ax = plt.subplots(figsize=(7, 5))
    
    # Filter out entries with None values
    filtered_rates = [(state, rate) for state, rate in readmission_rates if rate is not None]
    
    # Take top 10 from filtered list
    top_10 = filtered_rates[:10]
    
    if top_10:  # Check if we have any data
        states, rates = zip(*top_10)
        y_pos = np.arange(len(states))
        
        ax.bar(y_pos, rates, align='center', alpha=0.8)
        ax.set_xticks(y_pos)
        ax.set_xticklabels(states, rotation=90)
        ax.set_ylabel('Readmission Rate')
        ax.set_title('Top 10 Readmission Rates by State')
    else:
        ax.text(0.5, 0.5, "No valid readmission rate data available", 
                horizontalalignment='center', verticalalignment='center')
        ax.set_title('Readmission Rates by State')

    plt.tight_layout()
    return fig

if __name__ == "__main__":
    readmission_rates = get_readmission_rates()
    #print("Readmission rates:", readmission_rates)
    
    # Create and save the map
    map_fig = draw_map(readmission_rates)
    map_fig.savefig('us_readmission_rates.png', dpi=300, bbox_inches='tight')
    plt.close(map_fig)

    # Create and save the column chart
    chart_fig = draw_column_chart(readmission_rates)
    chart_fig.savefig('us_col_chart.png', dpi=300, bbox_inches='tight')
