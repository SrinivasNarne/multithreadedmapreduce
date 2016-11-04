/*
 * Author - Srinivas Narne <srinivasnarne@gmail.com>
 * Description - Program to display statistics of words from the input file
*/

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <math.h>
#define buffer_size1 5
#define buffer_size2 7


char *inputFileName;
int buffer1[5];
int numberOfMappers = 0;
int numberOfReducers = 0;
char *buffer1Helper[5][20];
char *buffer2Helper[7][20];
char *buffer3[5][100];
int buffer3Helper[5][100] = {0};
int add = 0;
int rem = 0;
int num = 0;
int add2 = 0;
int rem2 = 0;
int num2 = 0;
int add3 = 0;
int rem3 = 0;
int num3 = 0;
int nextEmpty = 0;

pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m3 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t c_mapper = PTHREAD_COND_INITIALIZER;
pthread_cond_t c_mapper2 = PTHREAD_COND_INITIALIZER;
pthread_cond_t c_pooler = PTHREAD_COND_INITIALIZER;
pthread_cond_t c_reducer = PTHREAD_COND_INITIALIZER;
pthread_cond_t c_last = PTHREAD_COND_INITIALIZER;
pthread_cond_t c_last2 = PTHREAD_COND_INITIALIZER;

void *pooler (void *param);
void *mapper (void *param);
void *reducer (void *param);
void *wordCountWriter (void *param);
void printBuffer1();
void printBuffer2();
void printBuffer3();

int main(int argc, char *argv[]) {
  inputFileName = argv[1];
  numberOfMappers = atoi(argv[2]);
  numberOfReducers = atoi(argv[3]);
	pthread_t tid1;
  pthread_t tid2[buffer_size1];
  pthread_t tid3[buffer_size2];
  pthread_t tid4;
  int i;
  //scanf("%s", inputFileName);
  FILE *fa = fopen("wordcount.txt", "w");
  fclose(fa);
  if(pthread_create(&tid1, NULL, pooler, NULL) != 0) {
		fprintf(stderr, "Unable to create pooler thread\n");
		exit(1);
	}
  for (i = 0; i < numberOfMappers; i ++) {
    if(pthread_create(&tid2[i], NULL, mapper, NULL) != 0) {
  		fprintf(stderr, "Unable to create mapper thread\n");
  		exit(1);
  	}
  }
  for (i = 0; i < numberOfReducers; i ++) {
    if(pthread_create(&tid3[i], NULL, reducer, NULL) != 0) {
  		fprintf(stderr, "Unable to create reducer thread\n");
  		exit(1);
  	}
  }
  if(pthread_create(&tid4, NULL, wordCountWriter, NULL) != 0) {
		fprintf(stderr, "Unable to create count writer thread\n");
		exit(1);
	}
  pthread_join(tid1, NULL);
  for (i = 0; i < numberOfMappers; i ++) {
    pthread_join(tid2[i], NULL);
  }
  for (i = 0; i < numberOfReducers; i ++) {
    pthread_join(tid3[i], NULL);
  }
  pthread_join(tid4, NULL);
	return 0;
}

void *pooler(void *param) {
  FILE * fp;
  char * line = NULL;
  size_t len = 0;
  ssize_t read;
  char firstChar = 0;
  char previousFirstChar = 0;
  fp = fopen(inputFileName, "r");
  if (fp == NULL)
    exit(EXIT_FAILURE);
  int i = 0;
  int temp = getline(&line, &len, fp);
  while (temp != -1) {
    line[strlen(line)-1] = 0;
    pthread_mutex_lock (&m);
    if (num > buffer_size1) {
      exit(1);
    }
    while (num == buffer_size1) {
      pthread_cond_wait (&c_pooler, &m);
    }
    buffer1Helper[add][0] = strdup(line);
    previousFirstChar = line[0];
    int index = 1;

    while ((temp = getline(&line, &len, fp)) != -1) {
      firstChar = line[0];
      if (previousFirstChar != firstChar) {
        while (index < 20) {
          buffer1Helper[add][index] = NULL;
          index ++;
        }
        break;
      }
      line[strlen(line)-1] = 0;
      buffer1Helper[add][index] = strdup(line);
      index ++;
      previousFirstChar = firstChar;
    }

    while (index < 20) {
      buffer1Helper[add][index] = NULL;
      index ++;
    }

    add = (add+1) % buffer_size1;
    num++;
    pthread_mutex_unlock (&m);
    //printBuffer1();
    pthread_cond_signal (&c_mapper);
    fflush (stdout);
  }
  free (line);
	return 0;
}

void *mapper(void *param) {
  int i;
  while(1) {
    pthread_mutex_lock (&m);
    if (num < 0) {
      exit(1);
    }
    while (num == 0) {
      pthread_cond_wait (&c_mapper, &m);
    }
    pthread_mutex_lock (&m2);
    if (num > buffer_size2) {
      exit(1);
    }
    while (num == buffer_size2) {
      pthread_cond_wait (&c_mapper2, &m2);
    }
    for (i = 0; i < 20; i ++) {
      if (buffer1Helper[rem][i] != NULL) {
        char *str1 = "(";
        char *str2 = strdup(buffer1Helper[rem][i]);
        char *str3 = ",1)";
        size_t len1 = strlen(str1);
        size_t len2 = strlen(str2);
        size_t len3 = strlen(str3);
        char *str4 = malloc(len1 + len2 + len3 + 1);
        str4 = strdup(str1);
        strcat(str4, str2);
        strcat(str4, str3);
        size_t len4 = strlen(str4);
        str4[len4 + 1] = '\0';
        buffer2Helper[add2][i] = strdup(str4);
      }
    }

    //printBuffer2();

    add2 = (add2 + 1) % buffer_size2;
    num2 ++;
    pthread_mutex_unlock (&m2);
    pthread_cond_signal (&c_reducer);

    rem = (rem+1) % buffer_size1;
    num--;
    pthread_mutex_unlock (&m);
    pthread_cond_signal (&c_pooler);
    fflush(stdout);
  }
  return 0;
 }

void *reducer(void *param) {
  int i,j;
  while (1) {
    pthread_mutex_lock (&m2);
    if (num2 < 0) {
      exit(1);
    }
    while (num2 == 0) {
      pthread_cond_wait (&c_reducer, &m2);
    }
    pthread_mutex_lock (&m3);
    if (num3 > 5) {
      exit(1);
    }
    while (num3 == 5) {
      pthread_cond_wait (&c_last, &m3);
    }
    for (i = 0; i < 20; i ++) {
      int found = 0;
      if (buffer2Helper[rem2][i] != NULL) {
        for (j = 0; j < nextEmpty; j ++) {
          if (strcmp(buffer2Helper[rem2][i], buffer3[add3][j]) == 0) {
            buffer3Helper[add3][j] = buffer3Helper[add3][j] + 1;
            found = 1;
          }
        }
        if (found == 0) {
          buffer3[add3][nextEmpty] = strdup(buffer2Helper[rem2][i]);
          buffer3Helper[add3][nextEmpty] = 1;
          nextEmpty ++;
        }
      }
    }
    nextEmpty = 0;
    add3 = (add3 + 1) % 5;
    num3 ++;

    //printBuffer3();

    pthread_mutex_unlock (&m3);
    pthread_cond_signal (&c_last2);
    rem2 = (rem2 + 1) % buffer_size2;
    num2--;
    pthread_mutex_unlock (&m2);
    pthread_cond_signal (&c_mapper2);
    fflush(stdout);
  }
  return 0;
}

void *wordCountWriter(void *param) {
  int i,j;
  while(1) {
    pthread_mutex_lock (&m3);
    if (num3 < 0) {
      exit(1);
    }
    while (num3 == 0) {
      pthread_cond_wait (&c_last2, &m3);
    }


    FILE *f = fopen("wordcount.txt", "a");
    if (f == NULL)
    {
      printf("Error opening file!\n");
      exit(1);
    }

    for (j = 0; j < 100; j ++) {
      if (buffer3[rem3][j] != NULL) {
        size_t len1 = strlen(buffer3[rem3][j]);
        char *temp = malloc(len1);
        temp = strdup(buffer3[rem3][j]);

        temp[len1- 1] = '\0';
        temp[len1 - 2] = '\0';
        fprintf(f, "%s%d)\n", temp, buffer3Helper[rem3][j]);
      }
    }

    rem3 = (rem3+1) % 5;
    num3 --;
    pthread_mutex_unlock (&m3);
    pthread_cond_signal (&c_last);
    fflush(stdout);
    fclose(f);
  }

}

void printBuffer1() {
  int i,j;
  for (i = 0; i < buffer_size1; i ++) {
    for (j = 0; j < 20; j ++) {
      printf("%s ", buffer1Helper[i][j]);
    }
    printf("\n");
  }
  printf("\n");
}

void printBuffer2() {
  int i,j;
  for (i = 0; i < buffer_size2; i ++) {
    for (j = 0; j < 20; j ++) {
      printf("%s ", buffer2Helper[i][j]);
    }
    printf("\n");
  }
  printf("\n");
}

void printBuffer3() {
  int i,j;
  for (i = 0; i < 5; i ++) {
    for (j = 0; j < 100; j ++) {
      if (buffer3[i][j] != NULL) {
        printf("%s %d", buffer3[i][j], buffer3Helper[i][j]);
      }
    }
    printf("\n");
  }
  printf("\n");
}
