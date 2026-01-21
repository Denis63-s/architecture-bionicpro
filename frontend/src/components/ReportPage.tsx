import React, { useState, useEffect } from 'react';
import { useKeycloak } from '@react-keycloak/web';

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reports, setReports] = useState<any[]>([]);
  const [status, setStatus] = useState<string>('');

  // Загрузить отчеты
  const loadReports = async () => {
    if (!keycloak?.token) return;

    try {
      setLoading(true);
      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (!response.ok) {
        throw new Error(`Error: ${response.status}`);
      }

      const data = await response.json();
      setReports(data.reports || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error loading reports');
    } finally {
      setLoading(false);
    }
  };

  // Генерация отчета
  const generateReport = async () => {
    if (!keycloak?.token) return;

    try {
      setGenerating(true);
      setError(null);
      setStatus('Starting generation...');

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports/generate`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `Error: ${response.status}`);
      }

      const data = await response.json();
      setStatus(data.message || 'Report generation started');
      
      // Обновить отчеты через 5 секунд
      setTimeout(() => {
        loadReports();
        setStatus('');
      }, 5000);

    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error generating report');
      setStatus('');
    } finally {
      setGenerating(false);
    }
  };

  // При загрузке
  useEffect(() => {
    if (keycloak?.authenticated) {
      loadReports();
    }
  }, [keycloak]);

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="w-full max-w-4xl p-8 bg-white rounded-lg shadow-md">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Usage Reports</h1>
          <button
            onClick={() => keycloak.logout()}
            className="px-3 py-1 text-gray-600 hover:text-gray-800"
          >
            Logout
          </button>
        </div>

        {/* Кнопки */}
        <div className="flex gap-4 mb-6">
          <button
            onClick={loadReports}
            disabled={loading}
            className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
              loading ? 'opacity-50 cursor-not-allowed' : ''
            }`}
          >
            {loading ? 'Loading...' : 'Refresh Reports'}
          </button>
          
          <button
            onClick={generateReport}
            disabled={generating}
            className={`px-4 py-2 bg-green-500 text-white rounded hover:bg-green-600 ${
              generating ? 'opacity-50 cursor-not-allowed' : ''
            }`}
          >
            {generating ? 'Generating...' : 'Generate New Report'}
          </button>
        </div>

        {/* Статус */}
        {status && (
          <div className="mb-4 p-3 bg-blue-100 text-blue-700 rounded">
            {status}
          </div>
        )}

        {/* Ошибки */}
        {error && (
          <div className="mb-4 p-3 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}

        {/* Отчеты */}
        <div>
          {reports.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              No reports found. Generate your first report.
            </div>
          ) : (
            <div className="space-y-4">
              {reports.map((report, index) => (
                <div key={index} className="p-4 border rounded">
                  <div className="flex justify-between mb-2">
                    <h3 className="font-bold">{report.customer?.name || 'Unknown User'}</h3>
                    <span className={`px-2 py-1 text-xs rounded ${
                      report.metadata?.data_freshness === 'fresh' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-yellow-100 text-yellow-800'
                    }`}>
                      {report.metadata?.data_freshness || 'unknown'}
                    </span>
                  </div>
                  
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <p><span className="font-medium">Email:</span> {report.customer?.email}</p>
                      <p><span className="font-medium">Segment:</span> {report.customer?.segment}</p>
                      <p><span className="font-medium">Hours Used:</span> {report.telemetry?.total_hours?.toFixed(1)}</p>
                    </div>
                    <div>
                      <p><span className="font-medium">Engagement:</span> {report.telemetry?.engagement_score?.toFixed(1)}</p>
                      <p><span className="font-medium">Lifetime Value:</span> ${report.financial?.lifetime_value?.toFixed(2)}</p>
                      <p><span className="font-medium">Last Updated:</span> {
                        report.metadata?.updated_at 
                          ? new Date(report.metadata.updated_at).toLocaleString() 
                          : 'Never'
                      }</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ReportPage;