import contextlib
import streamlit as st
import ibis
from ibis import _
import easier as ezr
import holoviews as hv
from holoviews import opts
from solarprod.ibis_tools import get_connections


hv.extension('bokeh')
opts.defaults(opts.Area(width=800, height=400), tools=[])
opts.defaults(opts.Curve(width=800, height=400, tools=['hover']))
opts.defaults(opts.Overlay(legend_position='top'))


def display(hv_obj):
    st.write(hv.render(hv_obj, backend='bokeh'))


ezr.mute_warnings()



with get_connections('local') as conn:
    detections = conn.table('detections')
    homeowner_ids = sorted(detections[['homeowner_id']].distinct().homeowner_id.execute())

ind2hid = {ind: hid for (ind, hid) in enumerate(homeowner_ids)}
hid2ind = {hid: ind for (ind, hid) in enumerate(homeowner_ids)}

# See the following for how to add button control to sliders
# See: https://docs.streamlit.io/knowledge-base/using-streamlit/widget-updating-session-state

# st.write(homeowner_ids)
homeowner_id = st.select_slider(
    'Homeowner ID', 
    options=homeowner_ids,
    # value=homeowner_ids[0],
    help='Select the homeowner Id you want',
    # on_change=save_current_hid,
    args=None,
    kwargs=None,
    key='hid_slider'
)




def move_slider(delta):
    ind = hid2ind[st.session_state.hid_slider] + delta
    if ind < 0:
        ind = len(homeowner_ids) - 1
    ind = ind % len(homeowner_ids)

    new_hid = ind2hid[ind]
    st.session_state.hid_slider = new_hid



col_tup = st.columns(7)
with col_tup[0]:
    st.button('previous', on_click=move_slider, args=(-1,),key='minus_one')
with col_tup[1]:
    st.button('Next', on_click=move_slider, args=(1,),key='plus_one')


with get_connections('local') as conn:
    nominal_prod = conn.table('nominal_prod')
    nominal_prod = nominal_prod[nominal_prod.homeowner_id == homeowner_id]
    detections = conn.table('detections')
    detections = detections[detections.homeowner_id == homeowner_id]

    raw_detections = conn.table('raw_detections')
    raw_detections = raw_detections[raw_detections.homeowner_id == homeowner_id]

    dfd = detections.execute()
    dfdr = raw_detections.execute()
    dfp = nominal_prod.execute()


st.markdown(f'### Detection {hid2ind[homeowner_id]} for homeowner_id = {homeowner_id}')
c1 = hv.Curve((dfp.date, dfp.total_production), label='Production').options(color='grey')
c2 = hv.Curve((dfp.date, dfp.nominal_prod), label='Nominal Production').options(color='black')
c3 = hv.Curve((dfp.date, dfp.baseline_nominal_prod), label='Baseline').options(color='green', alpha=.3)
c4 = hv.Scatter((dfd.date, dfd.nominal_prod), label='Detections').options(color='red', size=10)
c5 = hv.Scatter((dfdr.date, dfdr.nominal_prod), label='Detections').options(color='grey', size=10)
ol = c1 * c2 * c3 * c5 *  c4
display(ol)

dfd['date'] = [str(d.date()).replace('-', '_') for d in dfd.date]
dfdr['date'] = [str(d.date()).replace('-', '_') for d in dfdr.date]

st.markdown('### Detections')
st.dataframe(dfd)

st.markdown('### Raw Detections')
st.dataframe(dfdr)
