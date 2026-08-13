/*
 * normalize_omp_io.c
 * -----------------------------------------------------------------------
 * Misma lógica de normalización que normalize_omp.c, pero con el contrato
 * de E/S que espera handler.py: lee un array de floats desde un archivo
 * binario, normaliza en paralelo (OpenMP) y escribe el resultado a otro
 * archivo binario. Este es el binario referenciado por CPU_BINARY_PATH.
 *
 * Compilar:
 *   gcc -O3 -fopenmp -o normalize_omp_io normalize_omp_io.c -lm
 *
 * Uso:
 *   ./normalize_omp_io <archivo_entrada.bin> <archivo_salida.bin> <n_elementos>
 *
 * El binario CUDA (normalize_gpu, compilado desde normalize.cu con nvcc)
 * implementa exactamente el mismo contrato de E/S para que handler.py
 * pueda alternar entre GPU_BINARY_PATH y CPU_BINARY_PATH sin cambios.
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <omp.h>

int main(int argc, char** argv) {
    if (argc < 4) {
        fprintf(stderr, "Uso: %s <in.bin> <out.bin> <n_elementos>\n", argv[0]);
        return 1;
    }
    const char* inPath = argv[1];
    const char* outPath = argv[2];
    int n = atoi(argv[3]);

    float* data = malloc(sizeof(float) * n);
    FILE* fin = fopen(inPath, "rb");
    if (!fin) { perror("fopen entrada"); return 1; }
    size_t read = fread(data, sizeof(float), n, fin);
    fclose(fin);
    if ((int)read != n) {
        fprintf(stderr, "Advertencia: se esperaban %d elementos, se leyeron %zu\n", n, read);
    }

    float minVal = INFINITY, maxVal = -INFINITY;
    #pragma omp parallel for reduction(min:minVal) reduction(max:maxVal) schedule(static)
    for (int i = 0; i < n; i++) {
        if (data[i] < minVal) minVal = data[i];
        if (data[i] > maxVal) maxVal = data[i];
    }

    float range = maxVal - minVal;
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < n; i++) {
        data[i] = (range > 1e-8f) ? (data[i] - minVal) / range : 0.0f;
    }

    FILE* fout = fopen(outPath, "wb");
    if (!fout) { perror("fopen salida"); return 1; }
    fwrite(data, sizeof(float), n, fout);
    fclose(fout);

    free(data);
    return 0;
}
