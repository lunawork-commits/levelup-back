# LevOne — Полный гайд по API и архитектуре

---

## Оглавление

1. [Архитектура платформы](#1-архитектура-платформы)
2. [Роли и доступ](#2-роли-и-доступ)
3. [Система кулдаунов](#3-система-кулдаунов)
4. [Public API — общая схема](#4-public-api--общая-схема)
5. [Tenant API — ветка /api/v1/](#5-tenant-api--ветка-apiv1)
   - [Branches](#branches)
   - [Client](#client)
   - [Employees](#employees)
   - [Promotions](#promotions)
   - [Transactions](#transactions)
   - [VK Story](#vk-story)
   - [Testimonials (Отзывы)](#testimonials-отзывы)
   - [VK Callback](#vk-callback)
   - [Catalog (Магазин)](#catalog-магазин)
   - [Game (Мини-игра)](#game-мини-игра)
   - [Inventory (Инвентарь)](#inventory-инвентарь)
   - [Super Prize](#super-prize)
   - [Birthday (День рождения)](#birthday-день-рождения)
   - [Quests (Квесты)](#quests-квесты)
   - [Delivery (Доставка)](#delivery-доставка)
   - [Analytics API](#analytics-api)
6. [Telegram Webhook](#6-telegram-webhook)
7. [Панели администрирования](#7-панели-администрирования)
8. [Коды ошибок](#8-коды-ошибок)

---

## 1. Архитектура платформы

LevOne — мультиарендная платформа на базе **django-tenants** (PostgreSQL schemas).

### Как работает мультиарендность

```
Гость сканирует QR → запрос к public API → получает домен тенанта
→ все дальнейшие запросы идут на домен тенанта
→ django-tenants определяет схему БД по домену
→ запрос выполняется в изолированной схеме компании
```

**Две схемы:**
- **public** — общие данные: компании (тенанты), домены, глобальные гости (Client), пользователи (User), конфиги
- **tenant** — данные конкретной компании: точки (Branch), профили гостей (ClientBranch), монеты, инвентарь, квесты, каталог, рассылки

### Структура URL

| Домен | Схема | Назначение |
|-------|-------|------------|
| `levone.ru` (public) | public | `/api/v1/company/<id>/` — поиск тенанта |
| `*.levone.ru` (tenant) | tenant | `/api/v1/*` — все игровые эндпойнты |
| `levone.ru/superadmin/` | public | Панель суперадмина |
| `*.levone.ru/admin/` | tenant | Панель менеджера сети |
| `*.levone.ru/analytics/` | tenant | Аналитика |

---

## 2. Роли и доступ

### Иерархия ролей

| Роль | Флаг | Что может |
|------|------|-----------|
| `is_superuser=True` | Django superuser | Полный доступ везде, может создавать тенантов, управлять всеми пользователями |
| `role=superadmin` | — | Полный доступ к public_admin и всем tenant_admin, не может трогать superuser-пользователей |
| `role=network_admin` | — | Доступ к своим тенантам в public_admin (только просмотр компаний, нет доступа к пользователям/гостям) + полный доступ к своим tenant_admin |
| `role=client` | — | Только аналитика `/analytics/` и ответы на отзывы, без sidebar в admin |

### Как права работают технически

- `RoleBasedBackend` (`apps/shared/users/backends.py`) — кастомный бэкенд аутентификации. Заменяет стандартный `ModelBackend`. Даёт `has_perm=True` всем ролям `superadmin` и `network_admin`, не трогая Django Groups/Permissions.
- `is_staff=True` ставится автоматически всем пользователям при сохранении (нужен для `@staff_member_required` в аналитике).
- Django Groups и model-level permissions **не используются** — права управляются исключительно через `role`.

### Доступ к Admin панелям

**PublicAdminSite** (`/superadmin/`):

| Пользователь | Доступ |
|-------------|--------|
| `is_superuser` | Полный |
| `role=superadmin` | Полный, кроме: изменения superuser-профилей и других superadmin-пользователей, редактирования гостей |
| `role=network_admin` | Только свои компании (Company + ClientConfig), без пользователей и гостей |

**TenantAdminSite** (`/admin/`):

| Пользователь | Доступ |
|-------------|--------|
| `is_superuser` | Полный, без проверки компаний |
| `role=superadmin` | Полный доступ ко всем тенантам, без проверки компаний |
| `role=network_admin` | Только тенанты из списка companies, полный доступ внутри |
| `role=client` | Только страница с кодами дня + кнопки в аналитику, sidebar скрыт |

---

## 3. Система кулдаунов

Кулдаун — ограничение на повторное использование фичи. Один объект `Cooldown` на пару `(ClientBranch, feature)`.

| Фича | Длительность | Когда активируется |
|------|-------------|--------------------|
| `game` | 18 ч | После каждой игры |
| `shop` | 18 ч | После покупки в магазине за монеты |
| `inventory` | 18 ч | После активации приза из инвентаря (кроме ДР-призов) |
| `quest` | 18 ч | После активации квеста (независимо от результата) |

**Обход кулдауна игры:**
- С 3-й игры требуется код дня (`DailyCode`, purpose=`game`)
- Публикация VK-сторис сбрасывает кулдаун однократно

---

## 4. Public API — общая схема

**Base URL:** `https://levone.ru/api/v1/`

### GET `/api/v1/company/<client_id>/`

Находит тенанта по публичному ID компании. Вызывается при первом запуске мини-приложения.

**Path params**

| Param | Type | Описание |
|-------|------|----------|
| `client_id` | int | Публичный ID клиента (из QR-кода) |

**Response 200**

```json
{
  "domain": "kafe-centr.levone.ru",
  "name": "Кафе Центр"
}
```

**Ошибки:** `404 Company not found`, `403 Company inactive or subscription expired`

---

## 5. Tenant API — ветка /api/v1/

**Base URL:** `https://<tenant-domain>/api/v1/`

Все запросы и ответы в JSON. Аутентификация не требуется — тенант определяется по домену.

---

### Branches

#### GET `/api/v1/branches/<branch_id>/`

Возвращает контактную информацию и брендинг точки.

**Path params**

| Param | Type | Описание |
|-------|------|----------|
| `branch_id` | int | Integer ID из QR-кода |

**Response 200**

```json
{
  "id": 1,
  "branch_id": 42,
  "name": "Кафе Центр",
  "address": "ул. Ленина, 1",
  "phone": "+7 900 000 0000",
  "yandex_map": "https://yandex.ru/maps/...",
  "gis_map": "https://2gis.ru/...",
  "logotype_url": "/media/config/logos/logo.png",
  "coin_icon_url": "/media/config/coins/coin.png",
  "vk_group_id": 123456789,
  "vk_group_name": "Кафе Центр",
  "story_image_url": "/media/branch/stories/bg.jpg"
}
```

| Поле | Тип | Примечание |
|------|-----|-----------|
| `id` | int | DB PK |
| `branch_id` | int | Используется в QR-кодах |
| `logotype_url` | string\|null | Из настроек тенанта (платный брендинг) |
| `coin_icon_url` | string\|null | Из настроек тенанта (платный брендинг) |
| `vk_group_id` | int\|null | |
| `story_image_url` | string\|null | Шаблон для VK-сторис |

**Ошибки:** `404 Branch not found`, `403 Branch inactive`

---

### Client

#### GET `/api/v1/client/?vk_id=&branch_id=`

Возвращает профиль гостя.

**Query params:** `vk_id` (int, required), `branch_id` (int, required)

**Response 200** — объект [ClientProfile](#clientprofile-объект)

**Ошибки:** `404 Guest not found`

---

#### POST `/api/v1/client/`

Регистрация или вход гостя. Записывает визит (с кулдауном 6 ч).

**Request body**

```json
{
  "vk_id": 123456,
  "branch_id": 42,
  "first_name": "Иван",
  "last_name": "Иванов",
  "photo_url": "https://vk.com/...",
  "birth_date": "1990-05-15"
}
```

| Поле | Тип | Required |
|------|-----|----------|
| `vk_id` | int | ✓ |
| `branch_id` | int | ✓ |
| `first_name` | string | — |
| `last_name` | string | — |
| `photo_url` | URL | — |
| `birth_date` | date YYYY-MM-DD | — |

**Response 200** (существующий) / **201** (новый) — [ClientProfile](#clientprofile-объект)

**Ошибки:** `404 Branch not found`, `403 Branch inactive / guest blocked`

---

#### PATCH `/api/v1/client/`

Частичное обновление профиля гостя. Обновляются только переданные поля.

**Request body**

```json
{
  "vk_id": 123456,
  "branch_id": 42,
  "first_name": "Иван",
  "birth_date": "1990-05-15",
  "community_via_app": true,
  "newsletter_via_app": true
}
```

| Поле | Тип | Примечание |
|------|-----|-----------|
| `vk_id` | int | Required |
| `branch_id` | int | Required |
| `first_name` | string\|null | |
| `last_name` | string\|null | |
| `photo_url` | URL\|null | |
| `birth_date` | date\|null | |
| `community_via_app` | bool\|null | Отметить подписку на сообщество через приложение |
| `newsletter_via_app` | bool\|null | Отметить подписку на рассылку через приложение |

**Response 200** — [ClientProfile](#clientprofile-объект)

---

#### ClientProfile объект

```json
{
  "id": 1,
  "birth_date": "1990-05-15",
  "is_employee": false,
  "coins_balance": 250,
  "vk_id": 123456,
  "first_name": "Иван",
  "last_name": "Иванов",
  "photo_url": "https://vk.com/...",
  "is_community_member": true,
  "community_via_app": true,
  "is_newsletter_subscriber": false,
  "newsletter_via_app": null,
  "is_story_uploaded": false,
  "story_uploaded_at": null
}
```

| Поле | Примечание |
|------|-----------|
| `id` | ID объекта ClientBranch |
| `community_via_app` | `true` — подписался через приложение; `false` — был подписан до; `null` — не подписан |
| `newsletter_via_app` | Аналогично |

---

### Employees

#### GET `/api/v1/employees/?branch_id=`

Список сотрудников точки (is_employee=True). Нужен для выбора официанта в игре/квесте.

**Query params:** `branch_id` (int, required)

**Response 200**

```json
[
  {
    "id": 5,
    "birth_date": null,
    "coins_balance": 0,
    "vk_id": 789,
    "first_name": "Анна",
    "last_name": "Петрова",
    "photo_url": ""
  }
]
```

**Ошибки:** `404 Branch not found`, `403 Branch inactive`

---

### Promotions

#### GET `/api/v1/promotions/?branch_id=`

Все акции и скидки точки.

**Query params:** `branch_id` (int, required)

**Response 200**

```json
[
  {
    "id": 1,
    "title": "Скидка 20%",
    "discount": "−20% на всё меню",
    "dates": "01.06–30.06",
    "image_url": "/media/promotions/promo.jpg"
  }
]
```

---

### Transactions

#### GET `/api/v1/transactions/?vk_id=&branch_id=`

История монет гостя.

**Query params:** `vk_id` (int), `branch_id` (int)

**Response 200**

```json
[
  {
    "id": 10,
    "type": "income",
    "source": "game",
    "amount": 50,
    "description": "",
    "created_at": "2025-01-15T12:00:00Z"
  }
]
```

| `type` | Описание |
|--------|---------|
| `income` | Начисление |
| `expense` | Списание |

| `source` | Описание |
|----------|---------|
| `game` | Мини-игра |
| `quest` | Квест |
| `shop` | Магазин (покупка) |
| `birthday` | День рождения |
| `delivery` | Доставка |
| `manual` | Вручную менеджером |

---

### VK Story

#### POST `/api/v1/vk/story/`

Отметить публикацию VK-сторис гостем. Идемпотентный.

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42 }
```

**Response 200** (уже публиковал) / **201** (первая публикация)

```json
{
  "is_story_uploaded": true,
  "story_uploaded_at": "2025-01-15T14:30:00Z",
  "first_upload": true
}
```

**Ошибки:** `404 Guest not found`

---

### Testimonials (Отзывы)

#### POST `/api/v1/testimonials/`

Отправить отзыв из мини-приложения.

**Request body**

```json
{
  "vk_id": 123456,
  "branch_id": 42,
  "review": "Всё отлично!",
  "rating": 5,
  "phone": "+7 900 000 0000",
  "table": 3
}
```

| Поле | Тип | Required |
|------|-----|----------|
| `vk_id` | int | ✓ |
| `branch_id` | int | ✓ |
| `review` | string | ✓ |
| `rating` | int 1–5 | — |
| `phone` | string max 20 | — |
| `table` | int | — |

**Response 201**

```json
{ "detail": "Отзыв сохранён." }
```

**Ошибки:** `404 Branch not found`

---

### VK Callback

#### POST `/api/v1/vk/callback/`

Webhook для VK Callback API. Принимает события от ВКонтакте.

**Обрабатываемые события:**
- `confirmation` — возвращает строку подтверждения из `SenlerConfig.vk_callback_confirmation`
- `message_new` — сохраняет входящее сообщение в тред отзыва (TestimonialConversation)

**Верификация:** сопоставляет `secret` в теле запроса с `SenlerConfig.vk_callback_secret`.

**Response:** `200 ok`

---

### Catalog (Магазин)

#### GET `/api/v1/catalog/?branch_id=`

Все активные товары в магазине монет.

**Query params:** `branch_id` (int, required)

**Response 200**

```json
[
  {
    "id": 1,
    "name": "Кофе",
    "description": "Капучино",
    "image_url": "/media/catalog/coffee.jpg",
    "price": 100,
    "is_super_prize": false,
    "is_birthday_prize": false,
    "category_id": 2,
    "category_name": "Напитки"
  }
]
```

---

#### GET `/api/v1/catalog/cooldown/?vk_id=&branch_id=`

Статус кулдауна магазина.

**Response 200** — [CooldownStatus](#cooldownstatus-объект)

---

#### POST `/api/v1/catalog/buy/`

Купить товар за монеты. Списывает монеты, создаёт запись в инвентарь, запускает кулдаун `shop`.

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42, "product_id": 1 }
```

**Response 201** — [InventoryItem](#inventoryitem-объект)

**Ошибки:** `404 Guest / product not found`, `403 Shop on cooldown`, `400 Insufficient balance`

---

### Game (Мини-игра)

Игра реализована в **два этапа**: start → анимация на клиенте → claim.

#### POST `/api/v1/game/start/`

Фаза 1: открыть игровую сессию. Предопределяет награду, возвращает токен и балл анимации.

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42, "code": "12345" }
```

| Поле | Тип | Примечание |
|------|-----|-----------|
| `vk_id` | int | ✓ |
| `branch_id` | int | ✓ |
| `code` | string | Код дня (нужен с 3-й игры) |

**Response 200**

```json
{
  "session_token": "550e8400-e29b-41d4-a716-446655440000",
  "score": 7
}
```

`score` (1–10) — высота анимации на фронтенде.

**Специальный ответ — код нужен:**

```json
{ "needs_code": true }
```

**Ошибки:**
- `409` — кулдаун активен: `{ "expires_at": "...", "seconds_remaining": 64800 }`
- `400` — неверный код
- `404` — гость не найден

---

#### POST `/api/v1/game/claim/`

Фаза 2: зафиксировать результат после анимации. Токен действует 10 минут, защита от повтора.

**Request body**

```json
{ "session_token": "550e8400-...", "employee_id": 5 }
```

**Response 200** — одно из двух:

```json
// Обычная победа — монеты
{ "type": "coin", "reward": 50 }

// Суперприз (первая игра)
{
  "type": "super_prize",
  "reward": {
    "super_prize_id": 3,
    "available_products": [
      { "id": 7, "name": "Пицца", "image_url": "/media/pizza.jpg" }
    ]
  }
}
```

**Ошибки:** `400 Invalid or expired token`

---

#### GET `/api/v1/game/cooldown/?vk_id=&branch_id=`

Статус кулдауна игры.

**Response 200** — [CooldownStatus](#cooldownstatus-объект)

---

#### DELETE `/api/v1/game/cooldown/?vk_id=&branch_id=`

Сбросить кулдаун игры (отладка/ручной сброс).

**Response 204 No Content**

---

### Inventory (Инвентарь)

#### GET `/api/v1/inventory/?vk_id=&branch_id=`

Все предметы инвентаря гостя.

**Query params:** `vk_id` (int), `branch_id` (int)

**Response 200** — массив [InventoryItem](#inventoryitem-объект)

---

#### POST `/api/v1/inventory/activate/`

Активировать предмет для предъявления персоналу.

- **ДР-призы:** требуют код дня `birthday`, кулдаун не активируется
- **Остальные:** проверяется и активируется кулдаун `inventory` (18 ч)

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42, "item_id": 3, "code": "12345" }
```

| Поле | Примечание |
|------|-----------|
| `code` | Код дня (обязателен для ДР-призов) |

**Response 200** — [InventoryItem](#inventoryitem-объект)

**Ошибки:** `404 Item not found`, `409 Already activated / cooldown active`, `400 Invalid code`

---

#### GET `/api/v1/inventory/cooldown/?vk_id=&branch_id=`

Статус кулдауна инвентаря.

**Response 200** — [CooldownStatus](#cooldownstatus-объект)

---

#### InventoryItem объект

```json
{
  "id": 3,
  "product_id": 1,
  "product_name": "Кофе",
  "product_image_url": "/media/catalog/coffee.jpg",
  "acquired_from": "shop",
  "status": "pending",
  "duration": 86400,
  "activated_at": null,
  "expires_at": null,
  "created_at": "2025-01-15T12:00:00Z"
}
```

| `status` | Описание |
|----------|---------|
| `pending` | Ожидает активации |
| `active` | Активирован, таймер идёт |
| `used` | Использован |
| `expired` | Истёк |

| `acquired_from` | Описание |
|-----------------|---------|
| `shop` | Куплен за монеты |
| `game` | Выигран в игре |
| `quest` | Получен за квест |
| `birthday` | ДР-приз |
| `super_prize` | Суперприз |

---

### Super Prize

#### GET `/api/v1/super-prize/?vk_id=&branch_id=`

Список записей суперпризов гостя.

**Query params:** `vk_id` (int), `branch_id` (int)

**Response 200**

```json
[
  {
    "id": 1,
    "acquired_from": "game",
    "status": "pending",
    "created_at": "2025-01-15T12:00:00Z",
    "claimed_at": null,
    "product": null,
    "available_products": [
      { "id": 7, "name": "Пицца", "image_url": "/media/pizza.jpg" },
      { "id": 8, "name": "Бургер", "image_url": "/media/burger.jpg" }
    ]
  }
]
```

`available_products` присутствует только при `status=pending`.

---

#### POST `/api/v1/super-prize/`

Гость выбирает приз из пула суперпризов.

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42, "product_id": 7 }
```

**Response 200** — обновлённый SuperPrizeEntry

**Ошибки:** `404 Guest not found / no pending super prizes / product not found`

---

### Birthday (День рождения)

#### GET `/api/v1/birthday/status/?vk_id=&branch_id=`

Статус ДР-окна гостя.

**Response 200**

```json
{
  "is_birthday_window": true,
  "already_claimed": false,
  "can_claim": true
}
```

**Логика:**
- `is_birthday_window` — ±5 дней от дня рождения
- `can_claim` — `is_birthday_window` И дата рождения установлена ≥30 дней назад AND `already_claimed=false`

---

#### GET `/api/v1/birthday/prize/?vk_id=&branch_id=`

Список ДР-призов доступных для выбора.

**Response 200** — массив продуктов с `is_birthday_prize=True`

**Ошибки:** `403 Not in birthday window or birthday too recent`, `409 Already claimed this year`

---

#### POST `/api/v1/birthday/prize/`

Зарезервировать ДР-приз (создаёт pending-запись в инвентарь, активируется позже с кодом дня).

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42, "product_id": 9 }
```

**Response 201** — [InventoryItem](#inventoryitem-объект)

**Ошибки:** `403 Not in birthday window / birthday too recent`, `409 Already claimed this year`

---

### Quests (Квесты)

#### GET `/api/v1/quest/?vk_id=&branch_id=`

Все активные квесты с флагом выполнения для конкретного гостя.

**Response 200**

```json
[
  {
    "id": 1,
    "name": "Опубликуй сторис",
    "description": "Сфотографируй и опубликуй сторис с хэштегом",
    "reward": 500,
    "completed": false
  }
]
```

---

#### GET `/api/v1/quest/active/?vk_id=&branch_id=`

Текущий активный (в процессе) квест-сабмит гостя, или `{}` если нет.

**Response 200** — [QuestSubmit](#questsubmit-объект) или `{}`

---

#### POST `/api/v1/quest/activate/`

Запустить квест. Идемпотентен для уже активного квеста.

**Request body**

```json
{ "vk_id": 123456, "branch_id": 42, "quest_id": 1 }
```

**Response 201** (новый) / **200** (уже активен) — [QuestSubmit](#questsubmit-объект)

**Ошибки:** `404 Guest/quest not found`, `409 Already completed / on cooldown`

---

#### POST `/api/v1/quest/submit/`

Завершить квест (официант вводит код дня).

**Request body**

```json
{
  "vk_id": 123456,
  "branch_id": 42,
  "quest_id": 1,
  "code": "12345",
  "employee_id": 5
}
```

**Response 200** — [QuestSubmit](#questsubmit-объект)

**Ошибки:** `404 Guest or active quest not found`, `400 Invalid code`, `409 Quest expired`

---

#### GET `/api/v1/quest/cooldown/?vk_id=&branch_id=`

Статус кулдауна квестов.

**Response 200** — [CooldownStatus](#cooldownstatus-объект)

---

#### QuestSubmit объект

```json
{
  "id": 10,
  "quest_id": 1,
  "quest_name": "Опубликуй сторис",
  "quest_description": "Сфотографируй...",
  "quest_reward": 500,
  "status": "pending",
  "activated_at": "2025-01-15T12:00:00Z",
  "duration": 2400,
  "expires_at": "2025-01-15T12:40:00Z",
  "seconds_remaining": 1800
}
```

| `status` | Описание |
|----------|---------|
| `pending` | В процессе |
| `complete` | Выполнен |
| `expired` | Истёк таймер |

---

### Delivery (Доставка)

#### POST `/api/v1/webhook/delivery/`

Webhook от POS-систем — регистрирует код доставки. Идемпотентен.

**Аутентификация:** заголовок `X-Webhook-Secret` с секретным ключом.

**Request body**

```json
{ "source": "iiko", "branch_id": "uuid-организации", "code": "ORDER-ABC-12345" }
```

| `source` | Описание |
|----------|---------|
| `iiko` | Используется `Branch.iiko_organization_id` для поиска точки |
| `dooglys` | Используется `Branch.dooglys_branch_id` |

**Response 201** (новый) / **200** (уже существует) — Delivery объект

```json
{
  "id": 1,
  "short_code": "12345",
  "order_source": "iiko",
  "status": "pending",
  "expires_at": "2025-01-15T23:59:59Z",
  "activated_at": null
}
```

**Ошибки:** `403 Invalid signature`, `404 Branch not found`

---

#### POST `/api/v1/code/`

Гость вводит 5-значный код с чека для активации доставки.

**Request body**

```json
{ "short_code": "12345", "vk_id": 123456, "branch_id": 42 }
```

**Response 200** — Delivery объект

**Ошибки:** `404 Guest not found / code not found or expired`

---

### Analytics API

> Для панели администратора. Требуют авторизации (staff_member_required).

#### GET `/api/v1/analytics/branches/`

Список всех активных точек тенанта (для фильтра в дашборде).

**Response 200**

```json
[{ "id": 1, "name": "Кафе Центр" }]
```

---

#### GET `/api/v1/analytics/stats/`

Сводная статистика за период.

**Query params**

| Param | Тип | Описание |
|-------|-----|---------|
| `period` | string | `today`, `7d`, `30d`, `90d`, `year`, `all` (default: `30d`) |
| `start` | date | Кастомный старт (YYYY-MM-DD, перекрывает period) |
| `end` | date | Кастомный конец |
| `branch_ids` | string | Через запятую: `1,2,3` |

**Response 200**

```json
{
  "stats": { ... },
  "charts": { ... },
  "meta": {
    "start": "2025-01-01",
    "end": "2025-01-31",
    "branch_ids": [1, 2]
  }
}
```

---

#### GET `/api/v1/analytics/rf/`

RF-анализ: матрица сегментов, тренды, миграции.

**Query params**

| Param | Тип | Описание |
|-------|-----|---------|
| `mode` | string | `restaurant` \| `delivery` (default: `restaurant`) |
| `branch_ids` | string | Через запятую |
| `trend_days` | int | 7–365, период для графика трендов (default: 30) |
| `r_score` | int | 1–4, для детального просмотра ячейки матрицы |
| `f_score` | int | 1–3, для детального просмотра ячейки матрицы |

При передаче `r_score` + `f_score` возвращает список гостей в этом сегменте.

---

#### POST `/api/v1/analytics/rf/recalculate/`

Синхронный пересчёт RF-баллов (запускается вручную из дашборда).

**Request body**

```json
{ "mode": "restaurant", "branch_ids": "1,2" }
```

---

### CooldownStatus объект

```json
{
  "is_active": true,
  "expires_at": "2025-01-15T18:00:00Z",
  "seconds_remaining": 21600
}
```

---

### Reputation (Внешние отзывы Яндекс / 2ГИС)

Все эндпоинты требуют `IsAdminUser` (staff-session cookie, как и остальной admin API).
Схема определяется django-tenants по домену запроса.

#### GET `/api/v1/reputation/reviews/`

Список отзывов с площадок. Параметры query:

| Параметр | Тип | Описание |
|---|---|---|
| `branch`  | int  | Фильтр по `Branch.id` |
| `source`  | str  | `yandex` или `gis` |
| `status`  | str  | `new`/`seen`/`answered`/`ignored`, допускается список через запятую |
| `limit`   | int  | 1..200, default 50 |
| `offset`  | int  | default 0 |

Ответ:

```json
{
  "total": 42, "limit": 50, "offset": 0,
  "items": [
    {
      "id": 17,
      "branch": 3, "branch_name": "ЛевОне Ленина",
      "source": "yandex", "source_label": "Яндекс.Карты",
      "external_id": "yandex-42",
      "author_name": "Иван И.", "rating": 4,
      "text": "В целом понравилось, но...",
      "published_at": "2026-04-15T12:00:00Z",
      "status": "new", "status_label": "Новый",
      "reply_text": "", "replied_at": null,
      "reply_deeplink": "https://yandex.ru/maps/org/...",
      "fetched_at": "2026-04-16T04:00:00Z"
    }
  ]
}
```

#### POST `/api/v1/reputation/reviews/<id>/mark-seen/`

NEW → SEEN. Идемпотентно. Возвращает обновлённый ExternalReview.

#### POST `/api/v1/reputation/reviews/<id>/ignore/`

Помечает отзыв IGNORED. Возвращает обновлённый ExternalReview.

#### POST `/api/v1/reputation/reviews/<id>/save-reply/`

Сохраняет подготовленный ответ. Публикация на площадку — вручную через `reply_deeplink`.

Тело:

```json
{ "reply_text": "Спасибо за отзыв..." }
```

Ответ: обновлённый ExternalReview (status становится `answered`, `replied_at` — now).

#### POST `/api/v1/reputation/reviews/<id>/generate-reply/`

Просит Claude Haiku 4.5 сгенерировать черновик ответа. Ответ не сохраняется —
возвращается только текст.

- `503 Service Unavailable` — нет `ANTHROPIC_API_KEY`
- `502 Bad Gateway` — ошибка AI-провайдера

```json
{ "suggestion": "Здравствуйте, Иван! Спасибо, что поделились..." }
```

#### POST `/api/v1/reputation/sync/`

Диспатчит Celery-задачу `fetch_reviews_for_branch_task` на обновление отзывов.

Тело:

```json
{ "branch_id": 3, "source": "yandex" }
```

Если `source` не передан — синхронизируются оба источника. Ответ `202 Accepted`:

```json
{ "dispatched": ["yandex", "gis"], "schema": "levone" }
```

#### GET `/api/v1/reputation/sync-states/`

Состояние последней синхронизации по всем (branch × source):

```json
[
  {
    "id": 1, "branch": 3, "branch_name": "ЛевОне Ленина",
    "source": "yandex", "source_label": "Яндекс.Карты",
    "last_run_at": "2026-04-17T04:00:00Z",
    "last_ok_at":  "2026-04-17T04:00:00Z",
    "last_error": "",
    "reviews_fetched": 128
  }
]
```

---

### Public: Landing Settings (видео-модалка профиля)

#### GET `/api/v1/public/landing-settings/`

Публичный read-only эндпоинт. AllowAny, без авторизации.
Кэшируется на 5 минут (`Cache-Control: public, max-age=300`).

```json
{
  "is_enabled": true,
  "button_label": "ХОЧУ ЛЕВЕЛUP В СВОЁ КАФЕ",
  "title": "LevelUP для вашего кафе",
  "description": "Покажем, как это работает, за 30 секунд.",
  "video_url": "/media/landing/pitch.mp4",
  "poster_url": "/media/landing/posters/pitch.jpg",
  "cta_label": "Написать в Telegram",
  "cta_url": "https://t.me/LevelUP_bot",
  "updated_at": "2026-04-17T08:12:34Z"
}
```

Если `is_enabled=False` или `video_url` пуст — фронт показывает старую
Telegram-кнопку (поведение по умолчанию).

---

## 6. Telegram Webhook

#### GET/POST `/telegram/webhook/<bot_token>/`

Webhook для Telegram-бота (уведомления администраторов точки).

- **GET** — healthcheck, возвращает `200 OK`
- **POST** — обрабатывает Telegram Update (message, edited_message и др.)

**Request body (POST)**

```json
{
  "update_id": 123456789,
  "message": {
    "message_id": 42,
    "chat": { "id": 987654321 },
    "text": "/start"
  }
}
```

CSRF отключён (`@csrf_exempt`).

---

## 7. Панели администрирования

### /superadmin/ — Публичная панель

Доступна через public domain (e.g. `levone.ru/superadmin/`).

| URL | Описание |
|-----|---------|
| `/superadmin/` | Дашборд инфраструктуры (карточки компаний) |
| `/superadmin/clients/company/` | Управление компаниями-тенантами |
| `/superadmin/config/clientconfig/` | Конфиги клиентов (VK, POS, брендинг) |
| `/superadmin/users/user/` | Пользователи платформы |
| `/superadmin/guest/client/` | Глобальная база гостей |

### /admin/ — Панель тенанта

Доступна через tenant domain (e.g. `kafe.levone.ru/admin/`).

| URL | Описание |
|-----|---------|
| `/admin/` | Дашборд с кодами дня + ссылки на аналитику |
| `/admin/branch/branch/` | Торговые точки |
| `/admin/branch/branchconfig/` | Настройки точек |
| `/admin/branch/clientbranch/` | Профили гостей |
| `/admin/branch/cointransaction/` | Транзакции монет |
| `/admin/branch/dailycode/` | Коды дня |
| `/admin/catalog/product/` | Призы/товары |
| `/admin/catalog/productcategory/` | Категории призов |
| `/admin/quest/quest/` | Квесты |
| `/admin/senler/broadcast/` | Рассылки |
| `/admin/senler/autobroadcasttemplate/` | Шаблоны авторассылок |
| `/admin/senler/senlerconfig/` | VK-конфиг рассылок |
| `/admin/analytics/knowledgebasedocument/` | База знаний для ИИ |
| `/admin/ai/generate/` | POST — генерация текста через Claude AI |

### /analytics/ — Аналитика

Требует `is_staff=True`. Рендерит HTML-дашборды.

| URL | Описание |
|-----|---------|
| `/analytics/` | Общая статистика |
| `/analytics/rf/` | RF-анализ (матрица сегментов) |
| `/analytics/rf/migration/` | Миграции между RF-сегментами |
| `/analytics/reviews/` | Аналитика отзывов |
| `/analytics/reviews/detail/` | Список отзывов с фильтрами |
| `/analytics/reviews/reply/` | POST — ответить на отзыв через VK |
| `/analytics/stats/detail/` | Детальная статистика по метрике |

---

## 8. Коды ошибок

Все ошибки возвращаются в формате:

```json
{ "detail": "Описание ошибки." }
```

Ошибки валидации полей:

```json
{
  "vk_id": ["This field is required."],
  "branch_id": ["This field may not be null."]
}
```

| Status | Значение |
|--------|---------|
| `200` | OK |
| `201` | Created |
| `204` | No Content |
| `400` | Ошибка валидации (поля в теле ответа) |
| `403` | Доступ запрещён (неактивная точка, заблокированный гость, неверная подпись webhook) |
| `404` | Объект не найден |
| `409` | Конфликт (кулдаун активен, уже заявлено) |
| `500` | Внутренняя ошибка сервера |
