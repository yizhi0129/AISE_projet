#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>

#define PORT 6379
#define BUFFER_SIZE 1024
#define MAX_KEY_SIZE 256
#define HASH_TABLE_SIZE 1024

typedef struct node {
    char key[MAX_KEY_SIZE];
    char* value;
    struct node* next;
} node_t;

node_t* hashTable[HASH_TABLE_SIZE];

unsigned int hash(const char* key) {
    unsigned long int value = 0;
    while (*key != '\0') {
        value = value * 37 + *key++;
    }
    return value % HASH_TABLE_SIZE;
}

void setKeyValue(const char* key, const char* value) {
    unsigned int index = hash(key);
    node_t* node = hashTable[index];

    while (node != NULL && strncmp(node->key, key, MAX_KEY_SIZE) != 0) {
        node = node->next;
    }

    if (node != NULL) {
        free(node->value);
        node->value = strdup(value);
        return;
    }

    node = malloc(sizeof(node_t));
    if (node == NULL) {
        perror("Failed to allocate memory for node");
        return;
    }
    strncpy(node->key, key, MAX_KEY_SIZE);
    node->value = strdup(value);
    node->next = hashTable[index];
    hashTable[index] = node;
}

char* getKeyValue(const char* key) {
    unsigned int index = hash(key);
    node_t* node = hashTable[index];

    while (node != NULL) {
        if (strncmp(node->key, key, MAX_KEY_SIZE) == 0) {
            return node->value;
        }
        node = node->next;
    }
    return NULL;
}

void delKey(const char* key) {
    unsigned int index = hash(key);
    node_t* prev = NULL;
    node_t* node = hashTable[index];

    while (node != NULL && strncmp(node->key, key, MAX_KEY_SIZE) != 0) {
        prev = node;
        node = node->next;
    }

    if (node == NULL) {
        return;
    }

    if (prev == NULL) {
        hashTable[index] = node->next;
    }
    else {
        prev->next = node->next;
    }

    free(node->value);
    free(node);
}

void processCommand(int clientSocket, char* command) {
    char response[BUFFER_SIZE];
    if (strncmp(command, "SET", 3) == 0) {
        char key[MAX_KEY_SIZE], value[BUFFER_SIZE];
        sscanf(command, "SET %s %s", key, value);
        setKeyValue(key, value);
        snprintf(response, BUFFER_SIZE, "+OK\r\n");
    }
    else if (strncmp(command, "GET", 3) == 0) {
        char key[MAX_KEY_SIZE];
        sscanf(command, "GET %s", key);
        char* value = getKeyValue(key);
        if (value) {
            snprintf(response, BUFFER_SIZE, "$%s\r\n", value);
        }
        else {
            snprintf(response, BUFFER_SIZE, "$-1\r\n");
        }
    }
    else if (strncmp(command, "DEL", 3) == 0) {
        char key[MAX_KEY_SIZE];
        sscanf(command, "DEL %s", key);
        delKey(key);
        snprintf(response, BUFFER_SIZE, "+OK\r\n");
    }
    else {
        snprintf(response, BUFFER_SIZE, "-ERROR Unknown Command\r\n");
    }
    send(clientSocket, response, strlen(response), 0);
}

void* handleClient(void* clientSocket) {
    int sock = *((int*)clientSocket);
    free(clientSocket);
    char buffer[BUFFER_SIZE];

    while (recv(sock, buffer, BUFFER_SIZE, 0) > 0) {
        buffer[strlen(buffer) - 1] = '\0'; // Remove newline character
        processCommand(sock, buffer);
    }

    close(sock);
    return NULL;
}

int main() {
    int serverSocket, * clientSocket;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);
    pthread_t thread;

    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        perror("Failed to create socket");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Failed to bind to port");
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 5) < 0) {
        perror("Failed to listen on socket");
        exit(EXIT_FAILURE);
    }

    printf("Server is listening on port %d...\n", PORT);

    while (1) {
        clientSocket = malloc(sizeof(int));
        if (clientSocket == NULL) {
            perror("Failed to allocate memory for client socket");
            continue;
        }

        *clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientAddrLen);
        if (*clientSocket < 0) {
            perror("Failed to accept client connection");
            free(clientSocket);
            continue;
        }

        pthread_create(&thread, NULL, handleClient, (void*)clientSocket);
        pthread_detach(thread);
    }

    close(serverSocket);
    return 0;
}
