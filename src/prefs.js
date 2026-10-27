import { create } from 'zustand'

export const usePrefs = create((set) => ({
  client_snapshots: true,
  toggleClientSnapshots: () => set((state) => ({ client_snapshots: !(state.client_snapshots) })),
}))
