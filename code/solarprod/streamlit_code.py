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


# @contextlib.contextmanager
# def get_connection():
#     file_name = '/tmp/solar.ddb'
#     conn = ibis.duckdb.connect(file_name)
#     try:
#         yield conn
#     finally:
#         # conn.reconnect()
#         conn.con.dispose()

with get_connections('local') as conn:
    detections = conn.table('detections')
    homeowner_ids = sorted(detections[['homeowner_id']].distinct().homeowner_id.execute())

TRYNG TO GET BUTTOM UPDATES SO SLIDERS
See: https://docs.streamlit.io/knowledge-base/using-streamlit/widget-updating-session-state

# def get_current_hid():
#     return st.session_state['current_hid']


# if 'current_hid' not in st.session_state:
#     save_current_hid(homeowner_ids[0])

# def next_home():
#     if

# st.write(homeowner_ids)
homeowner_id = st.select_slider(
    'Homeowner ID', 
    options=homeowner_ids,
    value=homeowner_ids[0],
    help='Select the homeowner Id you want',
    # on_change=save_current_hid,
    args=None,
    kwargs=None,
)
# save_current_hid(homeowner_id)

st.write(homeowner_id)
# st.write(get_current_hid())

with get_connections('local') as conn:
    nominal_prod = conn.table('nominal_prod')
    nominal_prod = nominal_prod[nominal_prod.homeowner_id == homeowner_id]
    detections = conn.table('detections')
    detections = detections[detections.homeowner_id == homeowner_id]
    dfd = detections.execute()
    dfp = nominal_prod.execute()

c1 = hv.Curve((dfp.date, dfp.total_production), label='Production').options(color='grey')
c2 = hv.Curve((dfp.date, dfp.nominal_prod), label='Nominal Production').options(color='black')
c3 = hv.Scatter((dfd.date, dfd.nominal_prod), label='Detections').options(color='red', size=10)
ol = c1 * c2 * c3
display(ol)


# st.table(dfd)
# st.table(dfp)



# options = ['a', 'b', 'c', 'd', 'e', 'f', 'h', 1, 2, 3]
# options = list(range(1000))

# st.write('hello')

# with get_connection() as conn:
#     detections = conn.table('detections')
#     df = detections.head().execute()

# st.table(df)
    



# st.select_slider(
#     'mylabel', 
#     options=options, value=7,
#     help='this is help',
#     on_change=None,
#     args=None,
#     kwargs=None,
# )
