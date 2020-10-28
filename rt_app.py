import datetime

from bokeh.models import (BoxZoomTool, ColumnDataSource, DataRange1d,
                          HoverTool, PanTool, ResetTool, WheelZoomTool)
from bokeh.models.annotations import Span
from bokeh.palettes import Spectral6
from bokeh.plotting import curdoc, figure
from bokeh.transform import dodge, factor_cmap
from redis_tasks.task import TaskStatus
from redis_tasks.utils import utcnow

from history import get_tasks

time_window = datetime.timedelta(hours=48)
until_ts = utcnow() - time_window
worker_streams, short_tasks_stream = get_tasks(until_ts)
sources = {wnr: ColumnDataSource(gdf) for wnr, gdf in worker_streams.items()}
short_tasks_source = ColumnDataSource(short_tasks_stream)
x_range = DataRange1d(follow="end", follow_interval=time_window)
y_range = sorted(
    {k  for stream in sources.values() for k in stream.data['task_func']}, reverse=True)
statuses = [
    getattr(TaskStatus, status) for status in dir(TaskStatus) if status.isupper()]
root = figure(
    name="task_stream",
    title="Task Stream",
    id="bk-task-stream-plot",
    x_range=x_range,
    y_range=y_range,
    toolbar_location="above",
    x_axis_type="datetime",
    min_border_right=35,
    plot_height=600,
    plot_width=950,
    sizing_mode='scale_both',
    tools=''
)

xwheel = WheelZoomTool(dimensions='width')
hover = HoverTool(
    point_policy="follow_mouse",
    tooltips="""
        <div>
            <span style="font-size: 12px; font-weight: bold;">@key:</span>
            <span style="font-size: 12px; font-weight: bold;">@duration</span>
            <br/>
            <span style="font-size: 10px; font-family: Monaco, monospace;">@description</span>
        </div>
        """,
)


root.add_tools(
                # self.hover,
                ResetTool(),
                PanTool(),
                BoxZoomTool(),
                xwheel,
                hover
            )
root.toolbar.active_scroll = xwheel
for wnr, source in sources.items():
    rect = root.hbar(
        source=source,
        left="start",
        right="end",
        y=dodge("task_func", 0.25*wnr, range=root.y_range),
        height=0.2,
        fill_color=factor_cmap('status', palette=Spectral6, factors=statuses),
        legend_field='status',
        line_color="black",
        line_alpha=0.6,
        #fill_alpha="alpha",
        line_width=1,
    )
circles = root.circle(
    x="start", y="task_func", line_color="#3288bd",
    fill_color=factor_cmap('status', palette=Spectral6, factors=statuses),
    legend_field='status',
    line_width=1, size=8, source=short_tasks_source)

vline = Span(location=utcnow(), dimension='height', line_color='red', line_width=3)
root.add_layout(vline)

root.ygrid.grid_line_color = None
root.outline_line_color = None
root.legend.orientation = "horizontal"
root.legend.location = "top_center"

def refresh():
    until_ts = utcnow() - time_window
    worker_streams, short_tasks_stream = get_tasks(until_ts)
    for wnr, source in sources.items():
        source.data = worker_streams.get(wnr, {})
    short_tasks_source.data = short_tasks_stream
    vline.location = utcnow()
    task_names = sorted(
        {k  for stream22 in worker_streams.values()
             for k in worker_streams[0]['task_func']
        }, reverse=True)
    if (task_names != root.y_range.factors):
        root.y_range.factors = task_names




curdoc().add_periodic_callback(refresh, 1000)
curdoc().add_root(root)
