param(
    [string]$BaseUrl = "http://127.0.0.1:5000"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:Session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
$script:PassCount = 0
$script:FailCount = 0

function Write-Check {
    param(
        [string]$Name,
        [bool]$Ok,
        [string]$Details = ""
    )
    if ($Ok) {
        $script:PassCount++
        Write-Host "[PASS] $Name" -ForegroundColor Green
    } else {
        $script:FailCount++
        if ($Details) {
            Write-Host "[FAIL] $Name :: $Details" -ForegroundColor Red
        } else {
            Write-Host "[FAIL] $Name" -ForegroundColor Red
        }
    }
}

function Invoke-Api {
    param(
        [ValidateSet("GET", "POST")]
        [string]$Method,
        [string]$Path,
        $Body = $null
    )
    $uri = "$BaseUrl$Path"
    $status = 0
    $content = ""

    try {
        if ($null -eq $Body) {
            $resp = Invoke-WebRequest -Method $Method -Uri $uri -WebSession $script:Session -UseBasicParsing -TimeoutSec 25
        } else {
            $json = $Body | ConvertTo-Json -Depth 8
            $resp = Invoke-WebRequest -Method $Method -Uri $uri -WebSession $script:Session -UseBasicParsing -TimeoutSec 25 -ContentType "application/json" -Body $json
        }
        $status = [int]$resp.StatusCode
        $content = [string]$resp.Content
    } catch {
        $errResponse = $_.Exception.Response
        if ($null -eq $errResponse) {
            throw
        }
        $status = [int]$errResponse.StatusCode
        $stream = $errResponse.GetResponseStream()
        if ($null -ne $stream) {
            $reader = New-Object System.IO.StreamReader($stream)
            $content = $reader.ReadToEnd()
            $reader.Close()
        }
    }

    $data = $null
    if ($content) {
        try {
            $data = $content | ConvertFrom-Json
        } catch {
            $data = $null
        }
    }

    return [PSCustomObject]@{
        Status  = $status
        Data    = $data
        Content = $content
        Path    = $Path
    }
}

function Get-ReplyText {
    param($ApiResponse)
    if ($null -eq $ApiResponse -or $null -eq $ApiResponse.Data) {
        return ""
    }
    if ($ApiResponse.Data.PSObject.Properties.Name -contains "reply") {
        return [string]$ApiResponse.Data.reply
    }
    return ""
}

Write-Host "Running smoke tests against $BaseUrl" -ForegroundColor Cyan

# 1) Health
$health = Invoke-Api -Method GET -Path "/health"
Write-Check -Name "Health endpoint returns 200" -Ok ($health.Status -eq 200) -Details "Status=$($health.Status)"

# 2) Auth pre-checks
$meBefore = Invoke-Api -Method GET -Path "/auth/me"
$isAuthBefore = $false
if ($null -ne $meBefore.Data -and ($meBefore.Data.PSObject.Properties.Name -contains "authenticated")) {
    $isAuthBefore = [bool]$meBefore.Data.authenticated
}
Write-Check -Name "auth/me available" -Ok ($meBefore.Status -eq 200) -Details "Status=$($meBefore.Status)"
Write-Check -Name "User starts unauthenticated (fresh session)" -Ok (-not $isAuthBefore)

$notificationsBefore = Invoke-Api -Method GET -Path "/notifications"
Write-Check -Name "Notifications blocked before login (401)" -Ok ($notificationsBefore.Status -eq 401) -Details "Status=$($notificationsBefore.Status)"

# 3) Register + login
$suffix = Get-Date -Format "yyyyMMddHHmmss"
$username = "smoke_$suffix"
$password = "pass1234"

$reg = Invoke-Api -Method POST -Path "/auth/register" -Body @{ username = $username; password = $password }
Write-Check -Name "Register user" -Ok ($reg.Status -eq 200) -Details "Status=$($reg.Status) Body=$($reg.Content)"

$badLogin = Invoke-Api -Method POST -Path "/auth/login" -Body @{ username = $username; password = "wrongpass" }
Write-Check -Name "Bad login rejected (401)" -Ok ($badLogin.Status -eq 401) -Details "Status=$($badLogin.Status)"

$login = Invoke-Api -Method POST -Path "/auth/login" -Body @{ username = $username; password = $password }
Write-Check -Name "Login succeeds" -Ok ($login.Status -eq 200) -Details "Status=$($login.Status) Body=$($login.Content)"

$meAfter = Invoke-Api -Method GET -Path "/auth/me"
$isAuthAfter = $false
if ($null -ne $meAfter.Data -and ($meAfter.Data.PSObject.Properties.Name -contains "authenticated")) {
    $isAuthAfter = [bool]$meAfter.Data.authenticated
}
Write-Check -Name "User authenticated after login" -Ok ($meAfter.Status -eq 200 -and $isAuthAfter) -Details "Status=$($meAfter.Status)"

# 4) Chat + context
$hello = Invoke-Api -Method POST -Path "/get" -Body @{ message = "hello" }
$helloReply = Get-ReplyText -ApiResponse $hello
Write-Check -Name "Chat greeting works" -Ok ($hello.Status -eq 200 -and $helloReply.Length -gt 0) -Details "Status=$($hello.Status)"

$addApple = Invoke-Api -Method POST -Path "/get" -Body @{ message = "add apple 1" }
$addReply = Get-ReplyText -ApiResponse $addApple
Write-Check -Name "Add item works" -Ok ($addApple.Status -eq 200 -and ($addReply -match "Added|Total")) -Details "Reply=$addReply"

$addMore = Invoke-Api -Method POST -Path "/get" -Body @{ message = "add 2 more" }
$addMoreReply = Get-ReplyText -ApiResponse $addMore
Write-Check -Name "Context follow-up works (add 2 more)" -Ok ($addMore.Status -eq 200 -and ($addMoreReply -match "Added|Total")) -Details "Reply=$addMoreReply"

$recipePlan = Invoke-Api -Method POST -Path "/get" -Body @{ message = "i am making pasta tonight" }
$recipePlanReply = Get-ReplyText -ApiResponse $recipePlan
Write-Check -Name "Recipe planning works" -Ok ($recipePlan.Status -eq 200 -and ($recipePlanReply -match "For Pasta|For pasta|Great idea")) -Details "Reply=$recipePlanReply"

$addRecipe = Invoke-Api -Method POST -Path "/get" -Body @{ message = "add recipe pasta" }
$addRecipeReply = Get-ReplyText -ApiResponse $addRecipe
Write-Check -Name "Recipe add-all works" -Ok ($addRecipe.Status -eq 200 -and ($addRecipeReply -match "Added ingredients for Pasta|Total")) -Details "Reply=$addRecipeReply"

$cart = Invoke-Api -Method GET -Path "/cart"
$hasItems = $false
if ($null -ne $cart.Data -and ($cart.Data.PSObject.Properties.Name -contains "items")) {
    $hasItems = ($cart.Data.items.Count -ge 1)
}
Write-Check -Name "Cart endpoint returns items after add" -Ok ($cart.Status -eq 200 -and $hasItems) -Details "Status=$($cart.Status)"

# 5) Budget endpoint strict category filter
$budgetApi = Invoke-Api -Method GET -Path "/budget/optimize?budget=300&preferred_category=fruits"
$budgetOk = ($budgetApi.Status -eq 200)
$allFruit = $true
if ($null -eq $budgetApi.Data -or -not ($budgetApi.Data.PSObject.Properties.Name -contains "items")) {
    $allFruit = $false
} else {
    foreach ($it in $budgetApi.Data.items) {
        if ([string]$it.category -ne "fruits") {
            $allFruit = $false
            break
        }
    }
}
Write-Check -Name "Budget optimize endpoint returns only requested category" -Ok ($budgetOk -and $allFruit) -Details "Status=$($budgetApi.Status) Body=$($budgetApi.Content)"

# 6) Budget via chat strict category text check
$budgetChat = Invoke-Api -Method POST -Path "/get" -Body @{ message = "budget 300 fruits" }
$budgetChatReply = Get-ReplyText -ApiResponse $budgetChat
$badTerms = @("chips", "biscuit", "biscuits", "onion", "potato", "chocolate", "parle")
$containsWrong = $false
foreach ($t in $badTerms) {
    if ($budgetChatReply.ToLower().Contains($t)) {
        $containsWrong = $true
        break
    }
}
Write-Check -Name "Budget chat response excludes non-fruit items" -Ok ($budgetChat.Status -eq 200 -and -not $containsWrong) -Details "Reply=$budgetChatReply"

# 6b) Family shared list
$familyCreate = Invoke-Api -Method POST -Path "/get" -Body @{ message = "create family smoke home" }
$familyCreateReply = Get-ReplyText -ApiResponse $familyCreate
$createdOrExisting = ($familyCreate.Status -eq 200 -and ($familyCreateReply -match "Family created|already in family"))
Write-Check -Name "Family group create/join works" -Ok $createdOrExisting -Details "Reply=$familyCreateReply"

$familyAdd = Invoke-Api -Method POST -Path "/get" -Body @{ message = "family add milk 2" }
$familyAddReply = Get-ReplyText -ApiResponse $familyAdd
Write-Check -Name "Family add item works" -Ok ($familyAdd.Status -eq 200 -and ($familyAddReply -match "Added|family list")) -Details "Reply=$familyAddReply"

$familyList = Invoke-Api -Method POST -Path "/get" -Body @{ message = "family list" }
$familyListReply = Get-ReplyText -ApiResponse $familyList
Write-Check -Name "Family list fetch works" -Ok ($familyList.Status -eq 200 -and ($familyListReply -match "Shared Family List|family list")) -Details "Reply=$familyListReply"

# 7) Pickup time validation + order notifications
$confirm = Invoke-Api -Method POST -Path "/get" -Body @{ message = "confirm" }
$confirmReply = Get-ReplyText -ApiResponse $confirm
Write-Check -Name "Checkout asks fulfillment method" -Ok ($confirm.Status -eq 200 -and ($confirmReply -match "Pickup|Delivery")) -Details "Reply=$confirmReply"

$pickupMethod = Invoke-Api -Method POST -Path "/get" -Body @{ message = "pickup" }
$pickupMethodReply = Get-ReplyText -ApiResponse $pickupMethod
Write-Check -Name "Pickup method accepted" -Ok ($pickupMethod.Status -eq 200 -and ($pickupMethodReply -match "pickup time|HH:MM")) -Details "Reply=$pickupMethodReply"

$invalidPickup = Invoke-Api -Method POST -Path "/get" -Body @{ message = "00:01" }
$invalidPickupReply = Get-ReplyText -ApiResponse $invalidPickup
$pickupValidationOk = ($invalidPickup.Status -eq 200 -and ($invalidPickupReply -match "at least|Invalid time format|future"))
Write-Check -Name "Pickup time validation works" -Ok $pickupValidationOk -Details "Reply=$invalidPickupReply"

$validPickup = Invoke-Api -Method POST -Path "/get" -Body @{ message = "tomorrow 10:30 am" }
$validPickupReply = Get-ReplyText -ApiResponse $validPickup
Write-Check -Name "Valid pickup time places order" -Ok ($validPickup.Status -eq 200 -and ($validPickupReply -match "Order ID|Pickup scheduled")) -Details "Reply=$validPickupReply"

$notificationsAfter = Invoke-Api -Method GET -Path "/notifications"
$notifOk = ($notificationsAfter.Status -eq 200)
$notifText = ""
if ($null -ne $notificationsAfter.Data -and ($notificationsAfter.Data.PSObject.Properties.Name -contains "notifications")) {
    $notifText = (($notificationsAfter.Data.notifications | ConvertTo-Json -Depth 6) | Out-String)
}
Write-Check -Name "Notifications accessible after login" -Ok $notifOk -Details "Status=$($notificationsAfter.Status)"
Write-Check -Name "Order notification generated" -Ok ($notifText -match "Order Placed|Pickup Soon|Pickup Reminder") -Details $notifText

$insights = Invoke-Api -Method POST -Path "/get" -Body @{ message = "monthly insights" }
$insightsReply = Get-ReplyText -ApiResponse $insights
Write-Check -Name "Monthly insights message works" -Ok ($insights.Status -eq 200 -and ($insightsReply -match "Monthly Insights|No placed orders")) -Details "Reply=$insightsReply"

$familyOrders = Invoke-Api -Method POST -Path "/get" -Body @{ message = "family orders" }
$familyOrdersReply = Get-ReplyText -ApiResponse $familyOrders
Write-Check -Name "Family order timeline works" -Ok ($familyOrders.Status -eq 200 -and ($familyOrdersReply -match "Family Purchase Timeline|No placed family orders")) -Details "Reply=$familyOrdersReply"

$familyCheck = Invoke-Api -Method POST -Path "/get" -Body @{ message = "family check milk" }
$familyCheckReply = Get-ReplyText -ApiResponse $familyCheck
Write-Check -Name "Family item recency check works" -Ok ($familyCheck.Status -eq 200 -and ($familyCheckReply -match "Recent family purchases|No recent family purchase")) -Details "Reply=$familyCheckReply"

$familyStockScore = Invoke-Api -Method POST -Path "/get" -Body @{ message = "family stock score" }
$familyStockScoreReply = Get-ReplyText -ApiResponse $familyStockScore
Write-Check -Name "Family stock score works" -Ok ($familyStockScore.Status -eq 200 -and ($familyStockScoreReply -match "Family Stock Scoreboard|Not enough family purchase history")) -Details "Reply=$familyStockScoreReply"

$dupWarn = Invoke-Api -Method POST -Path "/get" -Body @{ message = "add 1 milk" }
$dupWarnReply = Get-ReplyText -ApiResponse $dupWarn
$dupGateOk = ($dupWarn.Status -eq 200 -and ($dupWarnReply -match "add anyway|Added"))
Write-Check -Name "Duplicate guard prompts override" -Ok $dupGateOk -Details "Reply=$dupWarnReply"

if ($dupWarnReply -match "add anyway") {
    $override = Invoke-Api -Method POST -Path "/get" -Body @{ message = "add anyway" }
    $overrideReply = Get-ReplyText -ApiResponse $override
    Write-Check -Name "Add anyway override applies pending item" -Ok ($override.Status -eq 200 -and ($overrideReply -match "Added Milk|Added")) -Details "Reply=$overrideReply"
}

Write-Host ""
Write-Host "Summary: PASS=$script:PassCount FAIL=$script:FailCount" -ForegroundColor Cyan
if ($script:FailCount -gt 0) {
    exit 1
}
exit 0
