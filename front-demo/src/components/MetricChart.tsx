import { useRef, useEffect } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

interface MetricData {
    chain_id: number;
    metric_name: string;
    granularity: string;
    period: string;
    value: number;
    computed_at: string;
}

interface MetricChartProps {
    metricName: string;
    data: MetricData[];
    granularity: string;
}

function formatDate(dateString: string, granularity: string): string {
    const date = new Date(dateString);

    switch (granularity) {
        case 'hour':
            return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' });
        case 'day':
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        case 'week':
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        case 'month':
            return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
        default:
            return date.toLocaleDateString('en-US');
    }
}

function formatMetricName(name: string, granularity: string): string {
    const baseName = name
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
    
    const capitalizedGranularity = granularity.charAt(0).toUpperCase() + granularity.slice(1);
    return `${baseName} per ${capitalizedGranularity}`;
}

function formatValue(value: number): string {
    if (value >= 1_000_000_000) {
        return (value / 1_000_000_000).toFixed(2) + 'B';
    }
    if (value >= 1_000_000) {
        return (value / 1_000_000).toFixed(2) + 'M';
    }
    if (value >= 1_000) {
        return (value / 1_000).toFixed(2) + 'K';
    }
    return value.toFixed(0);
}

function MetricChart({ metricName, data, granularity }: MetricChartProps) {
    const chartRef = useRef<HTMLDivElement>(null);
    const plotRef = useRef<uPlot | null>(null);

    useEffect(() => {
        if (!chartRef.current || !data || data.length === 0) return;

        // Prepare data for uPlot
        const timestamps = data.map(item => new Date(item.period).getTime() / 1000);
        const values = data.map(item => item.value);

        const opts: uPlot.Options = {
            width: chartRef.current.offsetWidth,
            height: 350,
            padding: [10, 10, 0, 5], // [top, right, bottom, left] - add padding for axes
            scales: {
                x: {
                    time: true,
                },
            },
            series: [
                {
                    label: 'Time',
                },
                {
                    label: formatMetricName(metricName, granularity),
                    stroke: '#3b82f6',
                    width: 2,
                    fill: 'rgba(59, 130, 246, 0.1)',
                    points: { show: false },
                },
            ],
            axes: [
                {
                    stroke: '#6b7280',
                    grid: { stroke: '#e5e7eb', width: 1 },
                    ticks: { stroke: '#e5e7eb' },
                    values: (_self, ticks) => {
                        return ticks.map(ts => {
                            const dateStr = new Date(ts * 1000).toISOString();
                            return formatDate(dateStr, granularity);
                        });
                    },
                    size: 90, // Reserve more vertical space for angled labels
                    rotate: -45, // Angle labels at -45 degrees
                },
                {
                    stroke: '#6b7280',
                    grid: { stroke: '#e5e7eb', width: 1 },
                    ticks: { stroke: '#e5e7eb' },
                    values: (_self, ticks) => ticks.map(v => formatValue(v)),
                    size: 100, // Reserve space for y-axis labels
                },
            ],
            cursor: {
                drag: {
                    x: false,
                    y: false,
                },
                points: {
                    size: 8,
                    width: 2,
                },
            },
        };

        const plotData: uPlot.AlignedData = [timestamps, values];
        const plot = new uPlot(opts, plotData, chartRef.current);
        plotRef.current = plot;

        // Handle resize
        const resizeObserver = new ResizeObserver(() => {
            if (plotRef.current && chartRef.current) {
                plotRef.current.setSize({ 
                    width: chartRef.current.offsetWidth, 
                    height: 350 
                });
            }
        });

        resizeObserver.observe(chartRef.current);

        return () => {
            resizeObserver.disconnect();
            plot.destroy();
        };
    }, [data, metricName, granularity]);

    if (!data || data.length === 0) {
        return (
            <div className="p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">{formatMetricName(metricName, granularity)}</h3>
                <p className="text-gray-500">No data available</p>
            </div>
        );
    }

    return (
        <div className="p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">{formatMetricName(metricName, granularity)}</h3>
            <div ref={chartRef} className="w-full" />
        </div>
    );
}

export default MetricChart;

