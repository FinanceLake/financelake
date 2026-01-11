import React from 'react';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

const StockChart = ({
  data,
  title,
  dataKey,
  dataKey2,
  color = '#8884d8',
  type = 'line'
}) => {
  // Format data for charts
  const formatData = (rawData) => {
    return rawData
      .map(item => ({
        ...item,
        timestamp: new Date(item.timestamp).getTime(),
        formattedTime: new Date(item.timestamp).toLocaleDateString(),
        formattedHour: new Date(item.timestamp).toLocaleTimeString()
      }))
      .sort((a, b) => a.timestamp - b.timestamp);
  };

  const chartData = formatData(data);

  const renderChart = () => {
    const commonProps = {
      data: chartData,
      margin: { top: 5, right: 30, left: 20, bottom: 5 }
    };

    switch (type) {
      case 'area':
        return (
          <AreaChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="formattedTime"
              tick={{ fontSize: 12 }}
              interval="preserveStartEnd"
            />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              labelFormatter={(label) => `Time: ${label}`}
              formatter={(value, name) => [
                typeof value === 'number' ? value.toFixed(2) : value,
                name === 'high_price' ? 'High' : name === 'low_price' ? 'Low' : name
              ]}
            />
            <Legend />
            <Area
              type="monotone"
              dataKey={dataKey}
              stackId="1"
              stroke={color}
              fill={color}
              fillOpacity={0.6}
            />
            {dataKey2 && (
              <Area
                type="monotone"
                dataKey={dataKey2}
                stackId="2"
                stroke="#ff7300"
                fill="#ff7300"
                fillOpacity={0.6}
              />
            )}
          </AreaChart>
        );

      case 'bar':
        return (
          <BarChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="formattedTime"
              tick={{ fontSize: 12 }}
              interval="preserveStartEnd"
            />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              labelFormatter={(label) => `Time: ${label}`}
              formatter={(value) => [
                typeof value === 'number' ? value.toLocaleString() : value,
                'Volume'
              ]}
            />
            <Legend />
            <Bar dataKey={dataKey} fill={color} />
          </BarChart>
        );

      default: // line
        return (
          <LineChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="formattedTime"
              tick={{ fontSize: 12 }}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 12 }}
              domain={['dataMin - 5', 'dataMax + 5']}
            />
            <Tooltip
              labelFormatter={(label) => `Time: ${label}`}
              formatter={(value) => [
                typeof value === 'number' ? value.toFixed(2) : value,
                'Price'
              ]}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey={dataKey}
              stroke={color}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
            />
          </LineChart>
        );
    }
  };

  return (
    <div className="chart-container">
      <h3 className="chart-title">{title}</h3>
      <div style={{ width: '100%', height: '300px' }}>
        <ResponsiveContainer>
          {renderChart()}
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default StockChart;
