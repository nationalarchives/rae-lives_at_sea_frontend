import { create } from 'zustand';

export const usePrefs = create((set) => ({
  client_snapshots: true,
  test_mode:        false,
  toggleClientSnapshots: () => set((state) => ({ client_snapshots: !(state.client_snapshots) })),
  toggleTestMode:        () => set((state) => ({ test_mode:        !(state.test_mode) })),
}));
