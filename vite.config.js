import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import eslint from 'vite-plugin-eslint'
import { copyFile, unlink, symlink } from 'node:fs/promises';
import { fileURLToPath, URL } from 'url'

// https://vite.dev/config/
export default defineConfig(({command, mode}) => {
  async function link_index() {
    if(command === 'serve' && mode === 'msw') {
      symlink('alt_dev_msw_index.jsx', 'src/index.jsx', 'file');
      await copyFile('node_modules/msw/lib/mockServiceWorker.js', 'public/mockServiceWorker.js');
    }
    else if(command === 'serve') {
      symlink('alt_dev_index.jsx', 'src/index.jsx', 'file');
    }
    else {
      symlink('real_index.jsx', 'src/index.jsx', 'file');
    }
  }

  //separate implementation so that it is crystal clear what happens in production
  function production_link_index() {
    symlink('real_index.jsx', 'src/index.jsx', 'file');
  }

  if(command === 'build' || mode === 'production') {
    return {
      build: {
        outDir: 'build',
      },
      plugins: [
        react(),
        {
          name: 'prebuild_production',
          buildStart(options) {
            //delete src/index.jsx if it exists. either way, create a link to a suitable implementation.
            unlink('src/index.jsx').then(production_link_index,
                                         production_link_index);
          },
        },
        eslint()
      ],
    }
  }
  else if(command === 'serve') {
    if(mode === 'test') {
      return {
        plugins: [react()],
        test: {
          environment: 'jsdom',
          //globals: true,
          setupFiles: './test/config/setupTests.js',
          include: ['./test/*.?(c|m)[jt]s?(x)'],
          pool: 'vmThreads',
        },
        resolve: {
          alias: {
            '@': fileURLToPath(new URL('./src', import.meta.url)),
          },
        },
      }
    }
    else if(mode === 'development' || mode === 'msw') {
      return {
        plugins: [
          react(),
          {
            name: 'prebuild',
            buildStart(options) {
              //delete src/index.jsx if it exists. either way, create a link to a suitable implementation.
              unlink('src/index.jsx').then(link_index,
                                           link_index);
            },
          },
          eslint(),
        ],
      }
    }
    else {
      throw Error('Check vite.config.js.\n       Command:' + command + '\n       Mode: ' + mode);
    }
  }
  throw Error('Check vite.config.js.\n       Command:' + command + '\n       Mode: ' + mode);
})
