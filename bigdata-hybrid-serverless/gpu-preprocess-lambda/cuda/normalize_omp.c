/*
 * normalize_omp.c
 * -----------------------------------------------------------------------
 * Variante CPU (OpenMP) de la normalización Min-Max, funcionalmente
 * equivalente al kernel CUDA (normalize.cu). Se usa para:
 *   (a) Fallback en instancias Lambda sin GPU disponible.
 *   (b) Referencia real de rendimiento CPU en el benchmark GPU vs CPU
 *       (este archivo SÍ se compiló y ejecutó en el entorno de desarrollo).
 *
 * Compilar:
 *   gcc -O3 -fopenmp -o normalize_omp normalize_omp.c -lm
 *
 * Uso:
 *   ./normalize_omp <n_elementos> <n_hilos>
 *   Imprime: n,threads,tiempo_reduccion_ms,tiempo_normalizacion_ms,tiempo_total_ms
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <omp.h>
#include <time.h>

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

void normalize_array_omp(float* data, int n) {
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
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <n_elementos> <n_hilos>\n", argv[0]);
        return 1;
    }
    int n = atoi(argv[1]);
    int threads = atoi(argv[2]);
    omp_set_num_threads(threads);

    float* data = malloc(sizeof(float) * n);
    srand(42);
    for (int i = 0; i < n; i++) {
        data[i] = (float)(rand() % 1000000) / 100.0f;
    }

    double t0 = now_ms();
    normalize_array_omp(data, n);
    double t1 = now_ms();

    // Verificación básica: rango final debe estar en [0,1]
    float finalMin = data[0], finalMax = data[0];
    for (int i = 1; i < n; i++) {
        if (data[i] < finalMin) finalMin = data[i];
        if (data[i] > finalMax) finalMax = data[i];
    }

    printf("%d,%d,%.4f,%.6f,%.6f\n", n, threads, t1 - t0, finalMin, finalMax);

    free(data);
    return 0;
}
