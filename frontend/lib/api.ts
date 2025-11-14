import axios from 'axios'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Document APIs
export const documentApi = {
  upload: async (file: File, fundId?: number) => {
    const formData = new FormData()
    formData.append('file', file)
    if (fundId) {
      formData.append('fund_id', fundId.toString())
    }
    
    const response = await api.post('/api/documents/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })
    return response.data
  },
  
  getStatus: async (documentId: number) => {
    const response = await api.get(`/api/documents/${documentId}/status`)
    return response.data
  },
  
  list: async (fundId?: number) => {
    const params = fundId ? { fund_id: fundId } : {}
    const response = await api.get('/api/documents/', { params })
    return response.data
  },
  
  delete: async (documentId: number) => {
    const response = await api.delete(`/api/documents/${documentId}`)
    return response.data
  },
}

// Fund APIs
export const fundApi = {
  list: async () => {
    const response = await api.get('/api/funds/')
    return response.data
  },
  
  get: async (fundId: number) => {
    const response = await api.get(`/api/funds/${fundId}`)
    return response.data
  },
  
  create: async (fund: any) => {
    const response = await api.post('/api/funds/', fund)
    return response.data
  },
  
  update: async (fundId: number, fund: any) => {
    const response = await api.put(`/api/funds/${fundId}`, fund)
    return response.data
  },

  delete: async (fundId: number) => {
    const response = await api.delete(`/api/funds/${fundId}`)
    return response.data
  },
  

  getTransactions: async (fundId: number, type: string, page: number = 1, limit: number = 50) => {
    const response = await api.get(`/api/funds/${fundId}/transactions`, {
      params: { transaction_type: type, page, limit }
    })
    return response.data
  },
  
  getMetrics: async (fundId: number) => {
    const response = await api.get(`/api/funds/${fundId}/metrics`)
    return response.data
  },
}

// Chat APIs
export const chatApi = {
  query: async (query: string, fundId?: number, conversationId?: string) => {
    const response = await api.post('/api/chat/query', {
      query,
      fund_id: fundId,
      conversation_id: conversationId,
    })
    return response.data
  },
  
  createConversation: async (fundId?: number) => {
    const response = await api.post('/api/chat/conversations', {
      fund_id: fundId,
    })
    return response.data
  },
  
  getConversation: async (conversationId: string) => {
    const response = await api.get(`/api/chat/conversations/${conversationId}`)
    return response.data
  },
}

// Metrics APIs
export const metricsApi = {
  getFundMetrics: async (fundId: number, metric?: string) => {
    const params = metric ? { metric } : {}
    const response = await api.get(`/api/metrics/funds/${fundId}/metrics`, { params })
    return response.data
  },
}
