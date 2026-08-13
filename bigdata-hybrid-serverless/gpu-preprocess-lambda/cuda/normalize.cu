/*
 * normalize.cu
 * -----------------------------------------------------------------------
 * Kernel CUDA para normalización Min-Max de un array numérico en GPU.
 * Forma parte del microservicio serverless de preprocesamiento (Etapa 1).
 *
 * Estrategia:
 *   1. Reducción paralela en GPU (dos kernels) para hallar min y max.
 *   2. Kernel de normalización: x' = (x - min) / (max - min)
 *   3. El host (C++) expone una función `normalize_array()` invocada
 *      desde el wrapper Lambda (ver src/gpu_handler.cpp) vía llamada
 *      a un binario nativo empaquetado en la capa (Lambda Layer) de AWS,
 *      ya que AWS Lambda no expone GPU directamente: se ejecuta sobre
 *      una instancia con GPU (EC2 g4dn) detrás de una función Lambda que
 *      actúa como orquestador remoto, o alternativamente sobre AWS
 *      Batch/SageMaker Processing invocado de forma asíncrona.
 *
 * Compilación (entorno con CUDA Toolkit instalado):
 *   nvcc -O3 -arch=sm_75 -o normalize_gpu normalize.cu
 *
 * NOTA DE ENTORNO: este sandbox de desarrollo no dispone de GPU ni del
 * CUDA Toolkit (nvcc), por lo que este archivo no pudo compilarse ni
 * ejecutarse aquí. El código sigue el patrón estándar de reducción CUDA
 * y fue validado sintácticamente; el benchmark real "GPU vs CPU" incluido
 * en el informe usa como referencia la variante OpenMP (normalize_omp.c),
 * que sí se compiló y ejecutó en este entorno, y proyecta el rendimiento
 * esperado en GPU a partir de la literatura de referencia (ver informe,
 * sección 5.1).
 */

#include <cuda_runtime.h>
#include <cfloat>
#include <cstdio>

#define BLOCK_SIZE 256

// ---------------------------------------------------------------------
// Kernel 1: reducción para min/max por bloque
// ---------------------------------------------------------------------
__global__ void reduceMinMaxKernel(const float* __restrict__ input, int n,
                                    float* blockMin, float* blockMax) {
    __shared__ float sMin[BLOCK_SIZE];
    __shared__ float sMax[BLOCK_SIZE];

    int tid = threadIdx.x;
    int idx = blockIdx.x * blockDim.x * 2 + tid;

    float localMin = FLT_MAX;
    float localMax = -FLT_MAX;

    if (idx < n) {
        localMin = localMax = input[idx];
    }
    if (idx + blockDim.x < n) {
        float v = input[idx + blockDim.x];
        localMin = fminf(localMin, v);
        localMax = fmaxf(localMax, v);
    }

    sMin[tid] = localMin;
    sMax[tid] = localMax;
    __syncthreads();

    for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (tid < stride) {
            sMin[tid] = fminf(sMin[tid], sMin[tid + stride]);
            sMax[tid] = fmaxf(sMax[tid], sMax[tid + stride]);
        }
        __syncthreads();
    }

    if (tid == 0) {
        blockMin[blockIdx.x] = sMin[0];
        blockMax[blockIdx.x] = sMax[0];
    }
}

// ---------------------------------------------------------------------
// Kernel 2: normalización Min-Max elemento a elemento
// ---------------------------------------------------------------------
__global__ void normalizeKernel(float* data, int n, float minVal, float maxVal) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        float range = maxVal - minVal;
        data[idx] = (range > 1e-8f) ? (data[idx] - minVal) / range : 0.0f;
    }
}

// ---------------------------------------------------------------------
// Host: orquesta la reducción + normalización sobre el arreglo completo
// ---------------------------------------------------------------------
extern "C" void normalize_array(float* h_data, int n) {
    float *d_data, *d_blockMin, *d_blockMax;
    int gridSize = (n + BLOCK_SIZE * 2 - 1) / (BLOCK_SIZE * 2);

    cudaMalloc(&d_data, n * sizeof(float));
    cudaMalloc(&d_blockMin, gridSize * sizeof(float));
    cudaMalloc(&d_blockMax, gridSize * sizeof(float));

    cudaMemcpy(d_data, h_data, n * sizeof(float), cudaMemcpyHostToDevice);

    reduceMinMaxKernel<<<gridSize, BLOCK_SIZE>>>(d_data, n, d_blockMin, d_blockMax);

    // Reducción final de los mínimos/máximos por bloque en host (arreglo pequeño)
    float* h_blockMin = new float[gridSize];
    float* h_blockMax = new float[gridSize];
    cudaMemcpy(h_blockMin, d_blockMin, gridSize * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy(h_blockMax, d_blockMax, gridSize * sizeof(float), cudaMemcpyDeviceToHost);

    float minVal = FLT_MAX, maxVal = -FLT_MAX;
    // Esta reducción final también podría paralelizarse con OpenMP si
    // gridSize es grande (miles de bloques); aquí se usa un pragma simple.
    #pragma omp parallel for reduction(min:minVal) reduction(max:maxVal)
    for (int i = 0; i < gridSize; i++) {
        minVal = fminf(minVal, h_blockMin[i]);
        maxVal = fmaxf(maxVal, h_blockMax[i]);
    }

    int normGrid = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    normalizeKernel<<<normGrid, BLOCK_SIZE>>>(d_data, n, minVal, maxVal);

    cudaMemcpy(h_data, d_data, n * sizeof(float), cudaMemcpyDeviceToHost);

    delete[] h_blockMin;
    delete[] h_blockMax;
    cudaFree(d_data);
    cudaFree(d_blockMin);
    cudaFree(d_blockMax);
}
